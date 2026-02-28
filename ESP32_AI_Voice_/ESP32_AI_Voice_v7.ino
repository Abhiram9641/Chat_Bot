/*
 * ══════════════════════════════════════════════════════════════════
 *  ESP32 AI Voice Assistant v4.1 — Sarvam AI STT Edition
 * ══════════════════════════════════════════════════════════════════
 *
 *  Hardware : ESP32 DOIT DevKit C · INMP441 · ILI9341 2.8" TFT
 *  STT      : Sarvam AI (api.sarvam.ai) — Saaras v3 model
 *  LLM      : Cerebras (api.cerebras.ai) — dual stream + fallback
 *
 *  Architecture:
 *  ┌────────────────────────────────────────────────────────────┐
 *  │            Watchdog-Supervised State Machine                │
 *  │  BOOT→IDLE→RECORDING→TRANSCRIBING→RESPONDING→IDLE         │
 *  │       ↕                   ↘ ERROR → auto-recover → IDLE   │
 *  │     SLEEP                                                  │
 *  ├─────────┬────────┬──────────┬────────┬────────┬───────────┤
 *  │  Audio  │  DSP   │ Network  │ Token  │Display │  Health   │
 *  │ Engine  │Pipeline│  Dual    │ Queue  │Engine  │  Monitor  │
 *  │         │        │  Mode    │(static)│        │           │
 *  │ I2S+DMA │DC+HP+  │Stream/  │Ring buf│Append  │Heap watch │
 *  │ WAV enc │Gate+   │Fallback │No alloc│+scroll │Self-heal  │
 *  │ Silence │PreEmph │Retry+   │Backpres│Status  │Uptime mgr │
 *  │ detect  │Integer │Timeout  │safe    │Sleep   │Error isol │
 *  └─────────┴────────┴──────────┴────────┴────────┴───────────┘
 *
 *  v4.1 Changes vs v4.0 (STT Provider Swap — Deepgram → Sarvam AI):
 *  ─────────────────────────────────────────────────────────────
 *   • Endpoint  : POST https://api.sarvam.ai/speech-to-text
 *   • Auth      : api-subscription-key header (not Authorization: Token)
 *   • Encoding  : multipart/form-data  (Deepgram used raw binary)
 *   • Model     : saaras:v3, mode=transcribe, language_code=unknown
 *   • Languages : 22 Indian languages + English — auto-detected
 *   • Response  : {"transcript":"...","language_code":"hi-IN",...}
 *   • CL calc   : Content-Length pre-computed from compile-time string
 *                 sizes + wav_total → zero extra heap alloc
 *   • WAV data  : streamed in 1024-byte chunks (unchanged pattern)
 *   • Response  : ArduinoJson parse with manual strstr() fallback
 *   • All retry / watchdog / Health:: / error-display logic intact
 *   • All DSP, Audio, LLM, UI, Memory, Sleep — 100% unchanged
 *
 *  v4.0 Hardening Features (all preserved):
 *   1. Dual-mode network: SSE streaming with non-streaming fallback
 *   2. Deterministic timeouts on every external operation
 *   3. Zero dynamic allocation in streaming path (static ring buf)
 *   4. Self-healing heap monitor with periodic cleanup
 *   5. Offline mode with graceful degradation
 *   6. True light-sleep with modem power management
 *   7. Backpressure-safe token queue (network→display decoupled)
 *   8. Subsystem-isolated error reporting
 *   9. Robust partial/malformed stream data handling
 *  10. Watchdog-supervised pipeline stages
 *  11. Socket force-teardown on stall detection
 *  12. Long-uptime stability (auto-restart after 6 hours)
 *  13. CPU-light idle (yield-based, no busy loops)
 *  14. Predictable UX under all network conditions
 *  15. Tagged structured logging for live demo debugging
 * ══════════════════════════════════════════════════════════════════
 */

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <driver/i2s.h>
#include <ArduinoJson.h>
#include <SPI.h>
#include <TFT_eSPI.h>
#include <esp_sleep.h>
#include <esp_wifi.h>
#include <esp_task_wdt.h>
#include <FS.h>
#include <SPIFFS.h>

// ══════════════════════════════════════════════════════════════
//  SECTION 1 — CONFIGURATION
// ══════════════════════════════════════════════════════════════
namespace Config {
  // ── Network ──
  constexpr const char* WIFI_SSID       = "Kanna";
  constexpr const char* WIFI_PASS       = "123456789";
  constexpr uint32_t    WIFI_TIMEOUT_MS = 15000;
  constexpr uint32_t    WIFI_RETRY_MS   = 10000;

  // ── Sarvam AI STT (replaces Deepgram) ──
  constexpr const char* SARVAM_KEY      = "sk_015v1cre_VeyfjOpZCvJKJmOGAtx5eEGn";
  constexpr const char* SARVAM_HOST     = "api.sarvam.ai";
  constexpr uint16_t    SARVAM_PORT     = 443;
  // Model: saaras:v3 = best accuracy, 22 Indian langs + English auto-detect
  constexpr const char* SARVAM_MODEL    = "saaras:v3";
  // Mode: transcribe = standard transcription in spoken language
  constexpr const char* SARVAM_MODE     = "transcribe";
  // language_code: unknown = let API auto-detect (Hindi, Telugu, Tamil, English, etc.)
  constexpr const char* SARVAM_LANG     = "unknown";

  // ── Cerebras LLM (unchanged) ──
  constexpr const char* CEREBRAS_KEY    = "csk-edkrty56d3re956xxr43nvxnp38j3e4ddjh9xxpncfkc4rxy";
  constexpr const char* LLM_MODEL       = "gpt-oss-120b";
  constexpr const char* LLM_HOST        = "api.cerebras.ai";
  constexpr uint16_t    LLM_PORT        = 443;

  // ── Audio ──
  constexpr uint32_t SAMPLE_RATE     = 8000;
  constexpr uint8_t  BIT_DEPTH       = 16;
  constexpr uint8_t  CHANNELS        = 1;
  // 8000 * 2 * 6 = 96000 bytes PCM + 44 WAV header = 96044 bytes total
  // Sarvam accepts WAV at any sample rate (best at 16kHz but 8kHz works fine)
  constexpr uint8_t  MAX_RECORD_SEC  = 6;
  constexpr uint16_t SILENCE_THRESH  = 160;
  constexpr uint16_t SILENCE_TAIL_MS = 900;
  constexpr uint16_t MIN_AUDIO_BYTES = 4000;

  // ── I2S Pins ──
  constexpr gpio_num_t I2S_WS   = GPIO_NUM_33;
  constexpr gpio_num_t I2S_SD   = GPIO_NUM_32;
  constexpr gpio_num_t I2S_SCK  = GPIO_NUM_14;
  constexpr i2s_port_t I2S_PORT = I2S_NUM_0;

  // ── Button ──
  constexpr uint8_t  BUTTON_PIN    = 4;
  constexpr uint16_t DEBOUNCE_MS   = 40;

  // ── Timing ──
  constexpr uint32_t SLEEP_TIMEOUT_MS   = 60000;
  constexpr uint32_t UPTIME_RESTART_MS  = 21600000; // 6 hours
  // Sarvam can be slightly slower than Deepgram for Indian language models
  constexpr uint32_t STT_TIMEOUT_MS     = 30000;
  constexpr uint32_t LLM_CONNECT_MS     = 12000;
  constexpr uint32_t LLM_HEADER_MS      = 15000;
  constexpr uint32_t LLM_STREAM_IDLE_MS = 10000;
  constexpr uint32_t LLM_TOTAL_MS       = 35000;
  constexpr uint32_t PIPELINE_MAX_MS    = 70000;    // +5s budget for Sarvam
  constexpr uint8_t  API_RETRIES        = 2;        // 3 total attempts (Sarvam is reliable)

  // ── Memory ──
  constexpr uint8_t  MAX_HISTORY     = 4;
  constexpr uint16_t MAX_MSG_LEN     = 300;
  constexpr uint16_t TOKEN_QUEUE_CAP = 64;
  constexpr uint16_t TOKEN_MAX_LEN   = 48;
  constexpr uint32_t HEAP_CRITICAL   = 20000;
  constexpr uint32_t HEAP_WARNING    = 35000;

  // ── Offline Responses ──
  constexpr uint8_t OFFLINE_RESP_COUNT = 5;
  constexpr const char* OFFLINE_RESPONSES[] = {
    "I'm offline right now. Please check the WiFi connection.",
    "No internet available. I'll respond once reconnected.",
    "Currently disconnected. Try again in a moment.",
    "I can't reach my brain right now. WiFi seems down.",
    "Offline mode. Press the button after WiFi reconnects."
  };
}

// ══════════════════════════════════════════════════════════════
//  SECTION 2 — COLOUR PALETTE
// ══════════════════════════════════════════════════════════════
namespace Pal {
  constexpr uint16_t BG           = 0x0841;
  constexpr uint16_t HEADER_BG    = 0x10A2;
  constexpr uint16_t HEADER_LINE  = 0x2945;
  constexpr uint16_t DIVIDER      = 0x18E3;

  constexpr uint16_t TEXT_TITLE   = 0xFFFF;
  constexpr uint16_t TEXT_USER    = 0xEF5D;
  constexpr uint16_t TEXT_AI      = 0x47F9;
  constexpr uint16_t LABEL_USER   = 0x9CF3;
  constexpr uint16_t LABEL_AI     = 0x0755;
  constexpr uint16_t TEXT_OFFLINE = 0xFB80;

  constexpr uint16_t S_IDLE       = 0x0247;
  constexpr uint16_t S_RECORD     = 0xA800;
  constexpr uint16_t S_PROC       = 0x6B40;
  constexpr uint16_t S_STREAM     = 0x0320;
  constexpr uint16_t S_ERROR      = 0xC000;
  constexpr uint16_t S_OFFLINE    = 0x4A49;
  constexpr uint16_t S_SLEEP      = 0x1082;
  constexpr uint16_t S_WARN       = 0xFBE0;

  constexpr uint16_t WIFI_OK      = 0x07E0;
  constexpr uint16_t WIFI_BAD     = 0xF800;
  constexpr uint16_t WIFI_WEAK    = 0xFD20;

  constexpr uint16_t MOON         = 0xFFE0;
  constexpr uint16_t STAR         = 0xFFFF;
  constexpr uint16_t STAR_DIM     = 0x6B6D;
  constexpr uint16_t SLEEP_BG     = 0x0000;
}

// ══════════════════════════════════════════════════════════════
//  SECTION 3 — ERROR SUBSYSTEM ISOLATION
// ══════════════════════════════════════════════════════════════
enum class ErrSource : uint8_t {
  NONE, AUDIO, WIFI, STT, LLM, DISP, HEAP, SYSTEM
};

static const char* errSourceName(ErrSource s) {
  switch (s) {
    case ErrSource::AUDIO:  return "AUDIO";
    case ErrSource::WIFI:   return "WIFI";
    case ErrSource::STT:    return "STT";
    case ErrSource::LLM:    return "LLM";
    case ErrSource::DISP:   return "DISPLAY";
    case ErrSource::HEAP:   return "HEAP";
    case ErrSource::SYSTEM: return "SYSTEM";
    default:                return "NONE";
  }
}

struct ErrorRecord {
  ErrSource source;
  char      message[60];
  uint32_t  timestamp;
  uint8_t   count;
};

static ErrorRecord g_last_error = {ErrSource::NONE, "", 0, 0};

// ══════════════════════════════════════════════════════════════
//  SECTION 4 — STATE MACHINE
// ══════════════════════════════════════════════════════════════
enum class AppState : uint8_t {
  BOOT,
  IDLE,
  RECORDING,
  TRANSCRIBING,
  RESPONDING,
  SLEEP,
  WIFI_RETRY,
  ERROR_DISPLAY
};

static AppState  g_state          = AppState::BOOT;
static AppState  g_prev_state     = AppState::BOOT;
static uint32_t  g_state_ts       = 0;
static uint32_t  g_last_activity  = 0;
static bool      g_wifi_ok        = false;
static uint32_t  g_wifi_retry_ts  = 0;
static bool      g_sleep_drawn    = false;
static uint32_t  g_error_until    = 0;
static uint32_t  g_pipeline_start = 0;
// Language detection — set after each successful STT call
static char      g_detected_lang[16] = "en-IN";
static bool      g_is_telugu         = false;

// Forward declaration
static void enterState(AppState next);

// ══════════════════════════════════════════════════════════════
//  SECTION 5 — STRUCTURED LOGGER
// ══════════════════════════════════════════════════════════════
namespace Log {

  enum Level : uint8_t { TRACE, DEBUG, INFO, WARN, ERR, FATAL };
  static Level min_level = Level::DEBUG;

  static void _prefix(Level lvl, const char* tag) {
    uint32_t ms = millis();
    uint32_t s  = ms / 1000;
    const char* ls = "";
    switch (lvl) {
      case TRACE: ls = "TRC"; break;
      case DEBUG: ls = "DBG"; break;
      case INFO:  ls = "INF"; break;
      case WARN:  ls = "WRN"; break;
      case ERR:   ls = "ERR"; break;
      case FATAL: ls = "FTL"; break;
    }
    Serial.printf("[%5u.%03u][%s][%-6s] ", s, ms % 1000, ls, tag);
  }

  static void _log(Level lvl, const char* tag, const char* fmt, va_list args) {
    if (lvl < min_level) return;
    _prefix(lvl, tag);
    char buf[180];
    vsnprintf(buf, sizeof(buf), fmt, args);
    Serial.println(buf);
  }

  static void trace(const char* tag, const char* fmt, ...) {
    va_list a; va_start(a, fmt); _log(TRACE, tag, fmt, a); va_end(a);
  }
  static void debug(const char* tag, const char* fmt, ...) {
    va_list a; va_start(a, fmt); _log(DEBUG, tag, fmt, a); va_end(a);
  }
  static void info(const char* tag, const char* fmt, ...) {
    va_list a; va_start(a, fmt); _log(INFO, tag, fmt, a); va_end(a);
  }
  static void warn(const char* tag, const char* fmt, ...) {
    va_list a; va_start(a, fmt); _log(WARN, tag, fmt, a); va_end(a);
  }
  static void error(const char* tag, const char* fmt, ...) {
    va_list a; va_start(a, fmt); _log(ERR, tag, fmt, a); va_end(a);
  }
  static void fatal(const char* tag, const char* fmt, ...) {
    va_list a; va_start(a, fmt); _log(FATAL, tag, fmt, a); va_end(a);
  }

  static void heap(const char* ctx) {
    _prefix(DEBUG, "HEAP");
    Serial.printf("%s — free:%u max_blk:%u min_ever:%u\n",
                  ctx, ESP.getFreeHeap(),
                  ESP.getMaxAllocHeap(), ESP.getMinFreeHeap());
  }

  static void state(AppState from, AppState to) {
    static const char* names[] = {
      "BOOT","IDLE","RECORDING","TRANSCRIBING",
      "RESPONDING","SLEEP","WIFI_RETRY","ERROR_DISP"
    };
    _prefix(INFO, "STATE");
    Serial.printf("%s -> %s\n", names[(uint8_t)from], names[(uint8_t)to]);
  }

  static void pipeline(uint32_t rec_ms, uint32_t stt_ms,
                       uint32_t llm_ms, uint32_t total_ms,
                       bool streamed, const char* transcript) {
    _prefix(INFO, "PIPE");
    Serial.printf("rec:%ums stt:%ums llm:%ums total:%ums mode:%s q:\"%.30s\"\n",
                  rec_ms, stt_ms, llm_ms, total_ms,
                  streamed ? "STREAM" : "FALLBACK", transcript);
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 6 — HEALTH MONITOR (Self-Healing)
// ══════════════════════════════════════════════════════════════
namespace Health {

  static uint32_t last_check_ms        = 0;
  static uint32_t boot_time_ms         = 0;
  static uint8_t  consecutive_api_fails = 0;
  static uint8_t  heap_warnings        = 0;

  static void init() {
    boot_time_ms  = millis();
    last_check_ms = millis();
  }

  static void check() {
    if (millis() - last_check_ms < 5000) return;
    last_check_ms = millis();

    uint32_t free_heap = ESP.getFreeHeap();

    if (free_heap < Config::HEAP_CRITICAL) {
      heap_warnings++;
      Log::warn("HEALTH", "CRITICAL heap: %u bytes (warn #%u)", free_heap, heap_warnings);
      if (heap_warnings >= 3) {
        Log::fatal("HEALTH", "Heap critically low %u times — restarting", heap_warnings);
        delay(500);
        ESP.restart();
      }
    } else if (free_heap < Config::HEAP_WARNING) {
      Log::warn("HEALTH", "Low heap: %u bytes", free_heap);
    } else {
      if (heap_warnings > 0) heap_warnings--;
    }

    if (millis() - boot_time_ms > Config::UPTIME_RESTART_MS) {
      Log::info("HEALTH", "Scheduled restart after %u hours",
                (millis() - boot_time_ms) / 3600000);
      delay(200);
      ESP.restart();
    }

    if (consecutive_api_fails >= 5) {
      Log::warn("HEALTH", "%u consecutive API fails — forcing WiFi reset",
                consecutive_api_fails);
      WiFi.disconnect(true);
      delay(500);
      consecutive_api_fails = 0;
    }
  }

  static void apiSuccess() { consecutive_api_fails = 0; }
  static void apiFail()    {
    consecutive_api_fails++;
    Log::debug("HEALTH", "API fail count: %u", consecutive_api_fails);
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 7 — DSP PIPELINE (Integer-Only)
// ══════════════════════════════════════════════════════════════
namespace DSP {

  static int32_t dc_offset   = 0;
  static int32_t hp_prev_in  = 0;
  static int32_t hp_prev_out = 0;
  static int32_t noise_floor = 200;
  static int32_t prev_sample = 0;

  static void reset() {
    dc_offset = 0; hp_prev_in = 0; hp_prev_out = 0;
    noise_floor = 200; prev_sample = 0;
    Log::trace("DSP", "Filters reset");
  }

  static void processBlock(int16_t* samples, size_t count) {
    if (count == 0) return;

    // Stage 1+2: DC removal + High-pass IIR
    for (size_t i = 0; i < count; i++) {
      int32_t raw = samples[i];
      dc_offset += (raw - dc_offset) >> 9;
      int32_t dc_rem = raw - dc_offset;
      int32_t hp = (248 * (hp_prev_out + dc_rem - hp_prev_in)) >> 8;
      hp_prev_in  = dc_rem;
      hp_prev_out = hp;
      samples[i] = (int16_t)constrain(hp, -32768, 32767);
    }

    // Stage 3: Adaptive noise gate
    int64_t sum_sq = 0;
    for (size_t i = 0; i < count; i++) {
      int32_t s = samples[i];
      sum_sq += s * s;
    }
    uint32_t mean_sq = (uint32_t)(sum_sq / count);
    uint32_t rms = 0;
    if (mean_sq > 0) {
      rms = mean_sq;
      for (int j = 0; j < 8; j++) {
        uint32_t prev = rms;
        rms = (rms + mean_sq / rms) >> 1;
        if (rms == prev) break;
      }
    }

    if ((int32_t)rms < noise_floor)
      noise_floor += ((int32_t)rms - noise_floor) >> 4;
    else
      noise_floor += ((int32_t)rms - noise_floor) >> 6;

    int32_t gate = noise_floor + (noise_floor >> 1) + (noise_floor >> 3);
    if ((int32_t)rms < gate) {
      for (size_t i = 0; i < count; i++) samples[i] >>= 2;
    }

    // Stage 4: Pre-emphasis
    for (size_t i = 0; i < count; i++) {
      int32_t cur = samples[i];
      int32_t emp = cur - ((248 * prev_sample) >> 8);
      prev_sample = cur;
      samples[i] = (int16_t)constrain(emp, -32768, 32767);
    }
  }

  static uint16_t computeRMS(const int16_t* samples, size_t count) {
    if (count == 0) return 0;
    int64_t sum = 0;
    for (size_t i = 0; i < count; i++) {
      int32_t s = samples[i]; sum += s * s;
    }
    uint32_t m = (uint32_t)(sum / count);
    if (m == 0) return 0;
    uint32_t r = m;
    for (int j = 0; j < 8; j++) {
      uint32_t prev = r;
      r = (r + m / r) >> 1;
      if (r == prev) break;
    }
    return (uint16_t)r;
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 8 — AUDIO ENGINE
// ══════════════════════════════════════════════════════════════
namespace Audio {

  static const uint32_t PCM_BYTES =
    Config::SAMPLE_RATE * (Config::BIT_DEPTH / 8) * Config::MAX_RECORD_SEC;
  static const uint32_t BUF_SIZE = PCM_BYTES + 44;

  static uint8_t*  buf             = nullptr;
  static uint32_t  pcm_size        = 0;
  static bool      ready           = false;
  static uint16_t  peak_rms        = 0;
  static uint32_t  rec_duration_ms = 0;

  static void writeWavHeader(uint8_t* hdr, uint32_t pcm_bytes) {
    uint32_t br = Config::SAMPLE_RATE * Config::CHANNELS * (Config::BIT_DEPTH / 8);
    uint16_t ba = Config::CHANNELS * (Config::BIT_DEPTH / 8);
    auto w32 = [&](int o, uint32_t v){ memcpy(hdr + o, &v, 4); };
    auto w16 = [&](int o, uint16_t v){ memcpy(hdr + o, &v, 2); };
    memcpy(hdr,      "RIFF", 4); w32(4,  pcm_bytes + 36);
    memcpy(hdr + 8,  "WAVE", 4);
    memcpy(hdr + 12, "fmt ", 4); w32(16, 16);
    w16(20, 1); w16(22, Config::CHANNELS);
    w32(24, Config::SAMPLE_RATE); w32(28, br);
    w16(32, ba); w16(34, Config::BIT_DEPTH);
    memcpy(hdr + 36, "data", 4); w32(40, pcm_bytes);
  }

  static bool begin() {
    const i2s_config_t cfg = {
      .mode              = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX),
      .sample_rate       = Config::SAMPLE_RATE,
      .bits_per_sample   = I2S_BITS_PER_SAMPLE_16BIT,
      .channel_format    = I2S_CHANNEL_FMT_ONLY_LEFT,
      .communication_format = I2S_COMM_FORMAT_STAND_I2S,
      .intr_alloc_flags  = ESP_INTR_FLAG_LEVEL1,
      .dma_buf_count     = 4,
      .dma_buf_len       = 256,
      .use_apll          = false
    };
    const i2s_pin_config_t pins = {
      .bck_io_num   = Config::I2S_SCK,
      .ws_io_num    = Config::I2S_WS,
      .data_out_num = I2S_PIN_NO_CHANGE,
      .data_in_num  = Config::I2S_SD
    };
    if (i2s_driver_install(Config::I2S_PORT, &cfg, 0, nullptr) != ESP_OK) {
      Log::fatal("I2S", "Driver install failed"); return false;
    }
    if (i2s_set_pin(Config::I2S_PORT, &pins) != ESP_OK) {
      Log::fatal("I2S", "Pin config failed"); return false;
    }
    Log::info("I2S", "OK: %uHz, 4x256 DMA", Config::SAMPLE_RATE);
    return true;
  }

  static void record() {
    pcm_size = 0; ready = false; peak_rms = 0;
    DSP::reset();
    i2s_zero_dma_buffer(Config::I2S_PORT);

    const size_t CHUNK = 512;
    uint32_t silence_start = 0;
    bool     got_voice     = false;
    uint32_t deadline      = millis() + Config::MAX_RECORD_SEC * 1000UL;
    uint32_t start         = millis();

    Log::info("REC", "Start (max %us)", Config::MAX_RECORD_SEC);

    while (digitalRead(Config::BUTTON_PIN) == LOW && millis() < deadline) {
      if (pcm_size + CHUNK > PCM_BYTES) break;

      size_t got = 0;
      esp_err_t err = i2s_read(Config::I2S_PORT,
                                buf + 44 + pcm_size,
                                CHUNK, &got, pdMS_TO_TICKS(80));
      if (err != ESP_OK || got == 0) continue;

      int16_t* samples = (int16_t*)(buf + 44 + pcm_size);
      size_t   sc      = got / 2;

      DSP::processBlock(samples, sc);

      uint16_t rms = DSP::computeRMS(samples, sc);
      if (rms > peak_rms) peak_rms = rms;

      if (rms > Config::SILENCE_THRESH) {
        got_voice = true; silence_start = 0;
      } else if (got_voice) {
        if (!silence_start) silence_start = millis();
        if (millis() - silence_start > Config::SILENCE_TAIL_MS) {
          Log::debug("REC", "Silence cutoff"); break;
        }
      }
      pcm_size += got;
    }

    rec_duration_ms = millis() - start;

    if (pcm_size >= Config::MIN_AUDIO_BYTES) {
      writeWavHeader(buf, pcm_size);
      ready = true;
      Log::info("REC", "Done: %ub, %ums, peak:%u", pcm_size, rec_duration_ms, peak_rms);
    } else {
      Log::warn("REC", "Too short: %u bytes", pcm_size);
    }
  }

  static bool allocate() {
    if (buf) return true;
    Log::heap("Audio alloc");
    if (ESP.getMaxAllocHeap() < BUF_SIZE + 8192) {
      Log::fatal("AUDIO", "Heap too small: need %u+8K", (unsigned)BUF_SIZE);
      return false;
    }
    buf = (uint8_t*)heap_caps_malloc(BUF_SIZE, MALLOC_CAP_8BIT);
    if (!buf) { Log::fatal("AUDIO", "malloc failed"); return false; }
    Log::info("AUDIO", "Buffer: %u bytes at %p", (unsigned)BUF_SIZE, buf);
    return true;
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 9 — TOKEN QUEUE (Zero-Allocation Backpressure Ring)
// ══════════════════════════════════════════════════════════════
namespace TokenQ {

  struct Token {
    char    text[Config::TOKEN_MAX_LEN];
    uint8_t len;
  };

  static Token             ring[Config::TOKEN_QUEUE_CAP];
  static volatile uint16_t head       = 0;
  static volatile uint16_t tail       = 0;
  static volatile bool     done_flag  = false;
  static volatile bool     error_flag = false;

  static void reset() {
    head = 0; tail = 0;
    done_flag = false; error_flag = false;
  }

  static uint16_t count() {
    int c = (int)head - (int)tail;
    if (c < 0) c += Config::TOKEN_QUEUE_CAP;
    return (uint16_t)c;
  }

  static bool isFull()  { return count() >= Config::TOKEN_QUEUE_CAP - 1; }
  static bool isEmpty() { return head == tail; }

  static bool push(const char* text, uint8_t len) {
    if (isFull()) {
      Log::trace("TKNQ", "Queue full — dropping token");
      return false;
    }
    uint8_t copy_len = (len >= Config::TOKEN_MAX_LEN) ? Config::TOKEN_MAX_LEN - 1 : len;
    memcpy(ring[head].text, text, copy_len);
    ring[head].text[copy_len] = '\0';
    ring[head].len = copy_len;
    __asm__ __volatile__("" ::: "memory");
    head = (head + 1) % Config::TOKEN_QUEUE_CAP;
    return true;
  }

  static bool pop(char* out, size_t out_size) {
    if (isEmpty()) return false;
    uint8_t len  = ring[tail].len;
    uint8_t copy = (len >= out_size) ? out_size - 1 : len;
    memcpy(out, ring[tail].text, copy);
    out[copy] = '\0';
    __asm__ __volatile__("" ::: "memory");
    tail = (tail + 1) % Config::TOKEN_QUEUE_CAP;
    return true;
  }

  static void signalDone()  { done_flag  = true; }
  static void signalError() { error_flag = true; }
  static bool isDone()      { return done_flag && isEmpty(); }
  static bool hasError()    { return error_flag; }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 10 — CONVERSATION MEMORY
// ══════════════════════════════════════════════════════════════
namespace Memory {
  struct Turn {
    char role[12];
    char content[Config::MAX_MSG_LEN];
  };
  static Turn    history[Config::MAX_HISTORY];
  static uint8_t count = 0;

  static void add(const char* role, const char* text) {
    if (count >= Config::MAX_HISTORY) {
      memmove(&history[0], &history[1], sizeof(Turn) * (Config::MAX_HISTORY - 1));
      count = Config::MAX_HISTORY - 1;
    }
    strlcpy(history[count].role,    role, sizeof(history[count].role));
    strlcpy(history[count].content, text, sizeof(history[count].content));
    count++;
    Log::debug("MEM", "+[%s] \"%.30s\" (%u turns)", role, text, count);
  }

  static void reset() { count = 0; }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 11 — NETWORK STACK
// ══════════════════════════════════════════════════════════════
namespace Net {

  static WiFiClientSecure* tls = nullptr;

  static bool connect() {
    WiFi.mode(WIFI_STA);
    WiFi.setSleep(false);
    WiFi.begin(Config::WIFI_SSID, Config::WIFI_PASS);
    Log::info("WIFI", "Connecting to '%s'...", Config::WIFI_SSID);

    uint32_t t = millis();
    while (WiFi.status() != WL_CONNECTED) {
      if (millis() - t > Config::WIFI_TIMEOUT_MS) {
        WiFi.disconnect(true);
        Log::error("WIFI", "Timeout (%ums)", Config::WIFI_TIMEOUT_MS);
        return false;
      }
      delay(150);
    }
    WiFi.setTxPower(WIFI_POWER_19_5dBm);
    Log::info("WIFI", "OK IP:%s RSSI:%d",
              WiFi.localIP().toString().c_str(), WiFi.RSSI());
    return true;
  }

  static bool isUp()  { return WiFi.status() == WL_CONNECTED; }
  static int  rssi()  { return isUp() ? WiFi.RSSI() : -100; }

  enum NetQuality : uint8_t { GOOD, FAIR, POOR, DOWN };
  static NetQuality quality() {
    if (!isUp()) return DOWN;
    int r = rssi();
    if (r > -60) return GOOD;
    if (r > -80) return FAIR;
    return POOR;
  }

  static void initTLS() {
    if (!tls) {
      tls = new WiFiClientSecure();
      tls->setInsecure();
      tls->setTimeout(10);
      Log::debug("TLS", "Client created");
    }
  }

  static void killTLS() {
    if (tls) {
      tls->stop();
      delay(50);
      delete tls;
      tls = nullptr;
      Log::debug("TLS", "Force-killed");
    }
  }

  static void enableModemSleep()  { esp_wifi_set_ps(WIFI_PS_MIN_MODEM); }
  static void disableModemSleep() { esp_wifi_set_ps(WIFI_PS_NONE); }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 12 — SARVAM AI STT
//  (Replaces Deepgram — multipart/form-data, api-subscription-key auth)
// ══════════════════════════════════════════════════════════════
namespace STT {

  // ────────────────────────────────────────────────────────────
  //  Multipart boundary segments (compile-time constants)
  //
  //  RFC 2046 multipart structure:
  //    --BOUNDARY\r\n<headers>\r\n\r\n<body>
  //    \r\n--BOUNDARY\r\n<headers>\r\n\r\n<body>
  //    ...
  //    \r\n--BOUNDARY--\r\n
  //
  //  We use "ESP32STTBound" as boundary — short to save bytes.
  //
  //  Content-Length = len(FILE_PART_HDR) + wav_total
  //                 + len(MODEL_PART) + len(MODE_PART)
  //                 + len(LANG_PART)  + len(END_BOUND)
  // ────────────────────────────────────────────────────────────

  // Part 1 header: file upload (no leading \r\n — it's the FIRST part)
  static const char FILE_PART_HDR[] =
    "--ESP32STTBound\r\n"
    "Content-Disposition: form-data; name=\"file\"; filename=\"rec.wav\"\r\n"
    "Content-Type: audio/wav\r\n"
    "\r\n";
  // Then wav binary data is written here...

  // Part 2: model field (leading \r\n = end-of-previous-part separator)
  static const char MODEL_PART[] =
    "\r\n--ESP32STTBound\r\n"
    "Content-Disposition: form-data; name=\"model\"\r\n"
    "\r\n"
    "saaras:v3";

  // Part 3: mode field
  static const char MODE_PART[] =
    "\r\n--ESP32STTBound\r\n"
    "Content-Disposition: form-data; name=\"mode\"\r\n"
    "\r\n"
    "transcribe";

  // Part 4: language_code field ("unknown" = auto-detect all 22 Indian langs + EN)
  static const char LANG_PART[] =
    "\r\n--ESP32STTBound\r\n"
    "Content-Disposition: form-data; name=\"language_code\"\r\n"
    "\r\n"
    "unknown";

  // Closing boundary
  static const char END_BOUND[] = "\r\n--ESP32STTBound--\r\n";

  // ── Helper: send a C-string over TLS in safe chunks ──
  // Avoids passing large buffers to write() which can stall on low-heap ESP32.
  static bool _tlsWriteStr(WiFiClientSecure& cl, const char* str) {
    size_t   total = strlen(str);
    size_t   sent  = 0;
    uint32_t dl    = millis() + 8000;  // 8s max for any text segment

    while (sent < total && millis() < dl) {
      size_t chunk   = (total - sent > 256) ? 256 : (total - sent);
      size_t written = cl.write((const uint8_t*)(str + sent), chunk);
      if (written == 0) {
        esp_task_wdt_reset();
        delay(5);
        continue;
      }
      sent += written;
    }
    if (sent != total) {
      Log::error("STT", "_tlsWriteStr incomplete: %u/%u", sent, total);
    }
    return (sent == total);
  }

  // ── Parse HTTP status + drain headers → return HTTP code ──
  static int _readStatusCode(WiFiClientSecure& cl) {
    String status = cl.readStringUntil('\n');
    status.trim();
    Log::debug("STT", "Status: '%s'", status.c_str());
    int sp = status.indexOf(' ');
    if (sp < 0) {
      Log::error("STT", "Malformed status line (len=%u)", status.length());
      return -1;
    }
    int code = status.substring(sp + 1, sp + 4).toInt();
    // Drain all response headers until blank line
    while (cl.connected() || cl.available()) {
      String hdr = cl.readStringUntil('\n');
      hdr.trim();
      if (hdr.length() == 0) break;
    }
    return code;
  }

  // ── Parse transcript from Sarvam JSON response body ──
  //
  //  Sarvam response format:
  //  {
  //    "request_id": "...",
  //    "transcript": "detected text here",
  //    "language_code": "hi-IN",
  //    "language_probability": 0.95
  //  }
  //
  //  Strategy:
  //   1. Read response body into static 800-byte buffer
  //   2. Try ArduinoJson first (clean parse)
  //   3. Fallback: manual strstr() search for "transcript"
  static bool _parseTranscript(WiFiClientSecure& cl,
                                char* out, size_t out_size) {
    out[0] = '\0';

    // Static body buffer — not on stack, not re-allocated each call
    static char body[800];
    size_t   body_len = 0;
    uint32_t deadline = millis() + 10000;  // 10s to read full response

    // Read until connection closes, timeout, or buffer full
    while (millis() < deadline) {
      if (cl.available()) {
        int ch = cl.read();
        if (ch < 0) break;
        if (body_len < sizeof(body) - 1) {
          body[body_len++] = (char)ch;
          body[body_len] = '\0';
        }
        // Once we have both "transcript" key and a closing brace we're done
        if (body_len > 30 && body[body_len - 1] == '}') break;
      } else if (!cl.connected()) {
        break;
      } else {
        delay(10);
      }
      esp_task_wdt_reset();
    }

    if (body_len == 0) {
      Log::error("STT", "Empty response body from Sarvam");
      return false;
    }

    Log::debug("STT", "Body(%u bytes): %.160s", body_len, body);

    // ── Strategy 1: ArduinoJson parse ──
    {
      StaticJsonDocument<512> doc;
      DeserializationError    jerr = deserializeJson(doc, body, body_len);
      if (!jerr) {
        const char* t = doc["transcript"] | "";
        if (strlen(t) > 0) {
          strlcpy(out, t, out_size);
          const char* lang = doc["language_code"] | "en-IN";
          Log::info("STT", "Detected lang: %s", lang);
          strlcpy(g_detected_lang, lang, sizeof(g_detected_lang));
          return true;
        }
        // transcript field present but empty = silence / no speech
        Log::warn("STT", "JSON OK but transcript empty (silence?)");
        return false;
      }
      Log::debug("STT", "ArduinoJson fail (%s) — trying manual parse",
                 jerr.c_str());
    }

    // ── Strategy 2: Manual strstr() fallback ──
    // Handles cases where response is chunked / slightly malformed JSON
    {
      const char* key = strstr(body, "\"transcript\"");
      if (key) {
        // Skip past   "transcript"   :   "
        const char* p = key + 12;           // skip "transcript"
        while (*p && *p != ':') p++;        // find colon
        if (*p == ':') p++;                 // skip colon
        while (*p == ' ' || *p == '\t') p++; // skip whitespace
        if (*p == '"') {
          p++;  // skip opening quote
          const char* end = p;
          // Scan forward, handle escaped quotes
          while (*end && *end != '"') {
            if (*end == '\\' && *(end + 1)) end++;  // skip escape
            end++;
          }
          size_t len = (size_t)(end - p);
          if (len > 0) {
            if (len >= out_size) len = out_size - 1;
            memcpy(out, p, len);
            out[len] = '\0';
            Log::info("STT", "Manual parse OK: len=%u", len);
            return true;
          }
        }
      }
    }

    Log::error("STT", "Transcript not found in: %.80s", body);
    return false;
  }

  // ════════════════════════════════════════════════════════════
  //  transcribe() — Main entry point
  //
  //  Sends WAV via multipart/form-data to Sarvam Saaras v3.
  //  Uses raw TLS socket to avoid HTTPClient's large heap alloc.
  //  Content-Length is pre-computed so we can send proper HTTP/1.1.
  //  WAV data is streamed in 1024-byte chunks from Audio::buf.
  //  Retries up to Config::API_RETRIES times with exponential backoff.
  // ════════════════════════════════════════════════════════════
  static bool transcribe(char* out, size_t out_size, uint32_t* elapsed_ms) {
    out[0] = '\0';
    uint32_t start = millis();

    // Pre-flight checks
    if (!Net::isUp()) {
      Log::error("STT", "WiFi down before STT");
      g_last_error = {ErrSource::WIFI, "WiFi down for STT", millis(), 1};
      if (elapsed_ms) *elapsed_ms = millis() - start;
      return false;
    }
    if (!Audio::ready || Audio::pcm_size == 0) {
      Log::error("STT", "No audio data for STT");
      g_last_error = {ErrSource::AUDIO, "No audio for STT", millis(), 1};
      if (elapsed_ms) *elapsed_ms = millis() - start;
      return false;
    }

    // Total WAV size = 44-byte header + PCM data
    const uint32_t wav_total = Audio::pcm_size + 44;

    // ── Pre-compute multipart/form-data Content-Length ──
    // This MUST be exact — HTTP/1.1 requires it before the body.
    // sizeof(X)-1 gives string length without null terminator.
    const uint32_t content_len =
        (sizeof(FILE_PART_HDR) - 1)  // part 1 header
      + wav_total                    // part 1 body (WAV binary)
      + (sizeof(MODEL_PART) - 1)     // part 2
      + (sizeof(MODE_PART)  - 1)     // part 3
      + (sizeof(LANG_PART)  - 1)     // part 4
      + (sizeof(END_BOUND)  - 1);    // closing boundary

    Log::info("STT", "Sarvam STT: wav=%u CL=%u model=%s mode=%s",
              wav_total, content_len,
              Config::SARVAM_MODEL, Config::SARVAM_MODE);

    for (int attempt = 0; attempt <= Config::API_RETRIES; attempt++) {

      // ── Exponential backoff (2s, 4s) ──
      if (attempt > 0) {
        uint32_t wait_ms = 2000UL << (attempt - 1);
        if (wait_ms > 8000) wait_ms = 8000;
        Log::warn("STT", "Retry %d/%d — backoff %ums",
                  attempt + 1, Config::API_RETRIES + 1, wait_ms);
        uint32_t bw = millis();
        while (millis() - bw < wait_ms) {
          esp_task_wdt_reset();
          delay(50);
        }
      }

      // ── Pipeline watchdog ──
      if (millis() - g_pipeline_start > Config::PIPELINE_MAX_MS) {
        Log::error("STT", "Pipeline timeout — aborting STT");
        break;
      }

      // ── Open raw TLS socket to api.sarvam.ai ──
      // We create a local (stack) WiFiClientSecure to keep lifecycle clean.
      // setInsecure() skips cert verify — saves ~4KB RAM on ESP32.
      WiFiClientSecure tls;
      tls.setInsecure();
      tls.setTimeout(35);  // 35s socket timeout — covers upload + response

      Log::debug("STT", "[att %d] Connecting %s:443...",
                 attempt + 1, Config::SARVAM_HOST);

      if (!tls.connect(Config::SARVAM_HOST, Config::SARVAM_PORT)) {
        Log::error("STT", "TLS connect failed (att %d, %ums elapsed)",
                   attempt + 1, millis() - start);
        Health::apiFail();
        tls.stop();
        continue;
      }
      Log::debug("STT", "TLS connected (%ums)", millis() - start);

      // ── HTTP Request Headers ──
      // IMPORTANT: send each header as a separate print() call.
      // A single large printf() string on low-heap ESP32 can be
      // silently truncated by the TLS send buffer, producing a
      // malformed request that Sarvam immediately rejects.
      tls.print("POST /speech-to-text HTTP/1.1\r\n");
      tls.print("Host: api.sarvam.ai\r\n");
      // Sarvam uses api-subscription-key header (NOT Authorization: Bearer/Token)
      tls.print("api-subscription-key: ");
      tls.print(Config::SARVAM_KEY);
      tls.print("\r\n");
      // Content-Type must include boundary parameter matching our parts
      tls.print("Content-Type: multipart/form-data; boundary=ESP32STTBound\r\n");
      // Pre-computed exact Content-Length
      char cl_buf[32];
      snprintf(cl_buf, sizeof(cl_buf), "Content-Length: %u\r\n", (unsigned)content_len);
      tls.print(cl_buf);
      tls.print("Connection: close\r\n");
      tls.print("\r\n");  // End of headers — body follows immediately

      Log::debug("STT", "HTTP headers sent, CL=%u, uploading...", content_len);

      // ── Multipart Body Part 1: File part header ──
      if (!_tlsWriteStr(tls, FILE_PART_HDR)) {
        Log::error("STT", "FILE_PART_HDR write failed (att %d)", attempt + 1);
        tls.stop(); Health::apiFail(); continue;
      }

      // ── Multipart Body Part 1: WAV binary data (chunked) ──
      {
        const uint8_t* ptr        = Audio::buf;  // points to WAV header + PCM
        uint32_t       remaining  = wav_total;
        uint32_t       stall_cnt  = 0;
        uint32_t       wav_dl     = millis() + 28000;  // 28s upload deadline

        while (remaining > 0 && millis() < wav_dl) {
          size_t chunk   = (remaining > 1024) ? 1024 : remaining;
          size_t written = tls.write(ptr, chunk);
          if (written == 0) {
            stall_cnt++;
            if (stall_cnt > 80) {
              Log::warn("STT", "Write stall (%u bytes remain)", remaining);
              break;
            }
            esp_task_wdt_reset();
            delay(5);
            continue;
          }
          stall_cnt  = 0;
          ptr       += written;
          remaining -= written;
          esp_task_wdt_reset();  // Keep WDT alive during large upload
        }

        if (remaining > 0) {
          Log::error("STT", "WAV upload incomplete: %u bytes unsent (att %d)",
                     remaining, attempt + 1);
          tls.stop(); Health::apiFail(); continue;
        }
        Log::debug("STT", "WAV uploaded (%ums)", millis() - start);
      }

      // ── Multipart Body Parts 2-4: text fields ──
      bool fields_ok =
          _tlsWriteStr(tls, MODEL_PART) &&
          _tlsWriteStr(tls, MODE_PART)  &&
          _tlsWriteStr(tls, LANG_PART)  &&
          _tlsWriteStr(tls, END_BOUND);

      if (!fields_ok) {
        Log::error("STT", "Text fields write failed (att %d)", attempt + 1);
        tls.stop(); Health::apiFail(); continue;
      }

      // CRITICAL: flush send buffer — ensures server receives full body
      // Without this the last TCP segment may sit in the ESP32 send queue.
      tls.flush();
      Log::debug("STT", "All parts sent + flushed (%ums)", millis() - start);

      // ── Wait for server response ──
      uint32_t resp_dl = millis() + Config::STT_TIMEOUT_MS;
      while (!tls.available() && millis() < resp_dl) {
        if (!tls.connected()) {
          Log::warn("STT", "Server closed connection before response");
          break;
        }
        esp_task_wdt_reset();
        delay(20);
      }

      if (!tls.available()) {
        Log::error("STT", "Response timeout (att %d, %ums elapsed)",
                   attempt + 1, millis() - start);
        tls.stop(); Health::apiFail(); continue;
      }

      // ── Read HTTP status line + headers ──
      int code = _readStatusCode(tls);
      Log::info("STT", "HTTP %d (att %d, %ums)", code, attempt + 1, millis() - start);

      if (code != 200) {
        // Drain error body for diagnostics
        char errbuf[200] = {0};
        int  ei = 0;
        uint32_t err_dl = millis() + 3000;
        while (tls.available() && ei < 199 && millis() < err_dl) {
          errbuf[ei++] = (char)tls.read();
          esp_task_wdt_reset();
        }
        errbuf[ei] = '\0';
        Log::error("STT", "HTTP %d body: %.120s", code, errbuf);
        tls.stop(); Health::apiFail(); continue;
      }

      // ── Parse transcript from JSON body ──
      bool ok = _parseTranscript(tls, out, out_size);
      tls.stop();

      if (!ok || out[0] == '\0') {
        Log::warn("STT", "No transcript returned (att %d) — silence or unknown lang",
                  attempt + 1);
        // Don't retry on empty transcript — likely silence, not a network error
        // But still mark as fail for Health stats
        Health::apiFail();
        continue;
      }

      // ── Success ──
      if (elapsed_ms) *elapsed_ms = millis() - start;
      Health::apiSuccess();
      Log::info("STT", "SUCCESS: \"%s\" (%ums)", out, millis() - start);
      return true;
    }

    // All attempts exhausted
    if (elapsed_ms) *elapsed_ms = millis() - start;
    g_last_error = {ErrSource::STT, "Sarvam STT failed", millis(), 1};
    Log::error("STT", "All %d attempts failed (%ums total)",
               Config::API_RETRIES + 1, millis() - start);
    return false;
  }
}  // namespace STT

// ══════════════════════════════════════════════════════════════
//  SECTION 13 — CEREBRAS LLM (Dual Mode: Stream + Fallback)
// ══════════════════════════════════════════════════════════════
namespace LLM {

  static bool g_streamed = false;

  static int buildPayload(const char* user_text, bool stream,
                          char* out, size_t out_size) {
    StaticJsonDocument<1280> doc;
    doc["model"]       = Config::LLM_MODEL;
    doc["max_tokens"]  = 250;
    doc["stream"]      = stream;
    doc["temperature"] = 0.7;

    JsonArray msgs = doc.createNestedArray("messages");
    JsonObject sys = msgs.createNestedObject();
    sys["role"]    = "system";
    sys["content"] = "You are a concise helpful AI assistant on an ESP32 "
                     "by Abhiram. Reply in 1-3 short sentences. Be warm and "
                     "direct. CRITICAL RULE: always reply in the SAME language "
                     "the user used. If user writes Telugu, reply in Telugu "
                     "script. If user writes English, reply in English.";

    for (uint8_t i = 0; i < Memory::count; i++) {
      JsonObject m = msgs.createNestedObject();
      m["role"]    = Memory::history[i].role;
      m["content"] = Memory::history[i].content;
    }

    JsonObject um = msgs.createNestedObject();
    um["role"]    = "user";
    um["content"] = user_text;

    return (int)serializeJson(doc, out, out_size);
  }

  static bool extractToken(const char* line, char* token, size_t tsize) {
    token[0] = '\0';
    if (strncmp(line, "data: ", 6) != 0) return false;
    const char* json = line + 6;
    if (strncmp(json, "[DONE]", 6) == 0) return false;
    if (strlen(json) < 10) return false;

    const char* key = strstr(json, "\"content\":");
    if (!key) return false;
    const char* val = key + 10;
    while (*val == ' ') val++;
    if (strncmp(val, "null", 4) == 0) return false;
    if (*val != '"') return false;
    val++;

    size_t i = 0;
    while (*val && *val != '"' && i < tsize - 1) {
      if (*val == '\\' && *(val + 1)) {
        val++;
        switch (*val) {
          case 'n':  token[i++] = '\n'; break;
          case 't':  token[i++] = ' ';  break;
          case '"':  token[i++] = '"';  break;
          case '\\': token[i++] = '\\'; break;
          case '/':  token[i++] = '/';  break;
          default:   token[i++] = *val; break;
        }
      } else {
        token[i++] = *val;
      }
      val++;
    }
    token[i] = '\0';
    return (i > 0);
  }

  static bool streamRequest(const char* user_text,
                            char* full_resp, size_t resp_size,
                            uint32_t* elapsed_ms) {
    full_resp[0] = '\0';
    uint32_t start = millis();
    g_streamed = true;

    char payload[1280];
    int  plen = buildPayload(user_text, true, payload, sizeof(payload));
    if (plen <= 0) { Log::error("LLM", "Payload build failed"); return false; }

    Net::initTLS();
    Log::info("LLM", "SSE connecting to %s...", Config::LLM_HOST);

    uint32_t conn_start = millis();
    if (!Net::tls->connect(Config::LLM_HOST, Config::LLM_PORT)) {
      Log::error("LLM", "TLS connect failed (%ums)", millis() - conn_start);
      Net::killTLS();
      return false;
    }
    Log::debug("LLM", "TLS connected (%ums)", millis() - conn_start);

    Net::tls->printf(
      "POST /v1/chat/completions HTTP/1.1\r\n"
      "Host: %s\r\n"
      "Authorization: Bearer %s\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: %d\r\n"
      "Accept: text/event-stream\r\n"
      "Connection: close\r\n\r\n%s",
      Config::LLM_HOST, Config::CEREBRAS_KEY, plen, payload);

    uint32_t hw = millis();
    while (!Net::tls->available()) {
      if (millis() - hw > Config::LLM_HEADER_MS) {
        Log::error("LLM", "Header timeout");
        Net::killTLS(); return false;
      }
      if (!Net::tls->connected()) {
        Log::error("LLM", "Disconnected waiting for headers");
        Net::killTLS(); return false;
      }
      delay(5);
    }

    String status = Net::tls->readStringUntil('\n');
    status.trim();
    Log::debug("LLM", "Status: %s", status.c_str());

    int http_code = 0;
    if (status.startsWith("HTTP/1.")) {
      int sp1 = status.indexOf(' ');
      if (sp1 > 0) http_code = status.substring(sp1 + 1, sp1 + 4).toInt();
    }

    while (Net::tls->connected()) {
      String hdr = Net::tls->readStringUntil('\n');
      hdr.trim();
      if (hdr.isEmpty()) break;
    }

    if (http_code != 200) {
      char errbuf[120] = {0}; int ei = 0;
      while (Net::tls->available() && ei < 119) errbuf[ei++] = Net::tls->read();
      errbuf[ei] = '\0';
      Log::error("LLM", "HTTP %d: %.80s", http_code, errbuf);
      Net::killTLS();
      return false;
    }

    Log::info("LLM", "SSE stream started");
    TokenQ::reset();

    uint32_t last_data    = millis();
    uint32_t stream_start = millis();
    size_t   resp_len     = 0;
    size_t   token_count  = 0;
    char     line[384];
    int      lpos         = 0;
    bool     stream_done  = false;
    bool     first_token  = true;

    while (!stream_done &&
           (millis() - last_data   < Config::LLM_STREAM_IDLE_MS) &&
           (millis() - stream_start < Config::LLM_TOTAL_MS)) {

      if (millis() - g_pipeline_start > Config::PIPELINE_MAX_MS) {
        Log::error("LLM", "Pipeline timeout — aborting stream");
        break;
      }

      if (!Net::tls->connected() && !Net::tls->available()) {
        Log::debug("LLM", "Connection closed by server");
        break;
      }

      while (Net::tls->available()) {
        char c = Net::tls->read();
        last_data = millis();

        if (c == '\n') {
          line[lpos] = '\0';
          if (lpos > 0 && line[lpos - 1] == '\r') line[--lpos] = '\0';

          if (lpos > 0) {
            if (strncmp(line, "data: [DONE]", 12) == 0) {
              stream_done = true;
              TokenQ::signalDone();
              break;
            }

            char token[Config::TOKEN_MAX_LEN];
            if (extractToken(line, token, sizeof(token))) {
              if (first_token) {
                Log::info("LLM", "TTFT: %ums", millis() - stream_start);
                first_token = false;
              }
              size_t tlen = strlen(token);
              if (resp_len + tlen < resp_size - 1) {
                memcpy(full_resp + resp_len, token, tlen);
                resp_len += tlen;
                full_resp[resp_len] = '\0';
              }
              TokenQ::push(token, tlen);
              token_count++;
            }
          }
          lpos = 0;
        } else {
          if (lpos < (int)sizeof(line) - 2) line[lpos++] = c;
        }
      }
      delay(1);
    }

    if (!stream_done) TokenQ::signalDone();
    Net::tls->stop();

    if (elapsed_ms) *elapsed_ms = millis() - start;

    if (resp_len > 0) {
      Log::info("LLM", "Stream OK: %u tokens, %u chars, %ums",
                token_count, resp_len, millis() - stream_start);
      Health::apiSuccess();
      return true;
    }

    Log::error("LLM", "Stream produced no tokens");
    return false;
  }

  static bool fallbackRequest(const char* user_text,
                              char* full_resp, size_t resp_size,
                              uint32_t* elapsed_ms) {
    full_resp[0] = '\0';
    uint32_t start = millis();
    g_streamed = false;

    Log::info("LLM", "Fallback (non-streaming) mode");

    char payload[1280];
    int  plen = buildPayload(user_text, false, payload, sizeof(payload));
    if (plen <= 0) return false;

    HTTPClient http;
    http.setTimeout(20000);
    http.setReuse(false);

    if (!http.begin("https://api.cerebras.ai/v1/chat/completions")) {
      Log::error("LLM", "HTTP begin failed");
      return false;
    }

    http.addHeader("Authorization", String("Bearer ") + Config::CEREBRAS_KEY);
    http.addHeader("Content-Type",  "application/json");

    int code = http.POST(payload);
    Log::debug("LLM", "Fallback HTTP %d", code);

    if (code != 200) {
      if (code > 0) {
        String body = http.getString();
        Log::error("LLM", "HTTP %d: %.80s", code, body.c_str());
      } else {
        Log::error("LLM", "HTTP err: %s", http.errorToString(code).c_str());
      }
      http.end();
      Health::apiFail();
      return false;
    }

    StaticJsonDocument<128> filter;
    filter["choices"][0]["message"]["content"] = true;

    DynamicJsonDocument resp(512);
    String body = http.getString();
    http.end();

    DeserializationError err = deserializeJson(
      resp, body, DeserializationOption::Filter(filter));
    if (err) {
      Log::error("LLM", "JSON parse error: %s", err.c_str());
      Health::apiFail();
      return false;
    }

    const char* ans = resp["choices"][0]["message"]["content"];
    if (!ans || strlen(ans) == 0) {
      Log::error("LLM", "Empty content in response");
      Health::apiFail();
      return false;
    }

    strlcpy(full_resp, ans, resp_size);
    if (elapsed_ms) *elapsed_ms = millis() - start;
    Health::apiSuccess();
    Log::info("LLM", "Fallback OK: %u chars, %ums", strlen(full_resp), millis() - start);
    return true;
  }

  static bool ask(const char* user_text,
                  char* full_resp, size_t resp_size,
                  uint32_t* elapsed_ms,
                  bool force_fallback = false) {

    bool try_stream = !force_fallback;
    if (try_stream && Net::quality() == Net::POOR) {
      Log::info("LLM", "Weak signal (RSSI %d) — using fallback", Net::rssi());
      try_stream = false;
    }

    if (try_stream) {
      if (streamRequest(user_text, full_resp, resp_size, elapsed_ms)) return true;
      Log::warn("LLM", "Stream failed — trying fallback");
      Net::killTLS();
      delay(200);
    }

    return fallbackRequest(user_text, full_resp, resp_size, elapsed_ms);
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 14 — UI ENGINE v4.0
// ══════════════════════════════════════════════════════════════
namespace UI {

  constexpr int W        = 320;
  constexpr int H        = 240;
  constexpr int HDR_H    = 28;
  constexpr int SB_H     = 28;
  constexpr int SB_Y     = H - SB_H;
  constexpr int CHAT_TOP = HDR_H + 2;
  constexpr int CHAT_BOT = SB_Y - 1;
  constexpr int CHAT_H   = CHAT_BOT - CHAT_TOP;
  constexpr int LABEL_W  = 28;
  constexpr int TEXT_X   = LABEL_W + 6;
  constexpr int TEXT_MAX = W - TEXT_X - 6;
  constexpr int LINE_H   = 16;
  constexpr int MSG_GAP  = 6;

  static TFT_eSPI tft;

  // ── Smooth-font (Telugu) support ──
  // g_smooth_font_loaded: set true when NotoTelugu16.vlw loads from SPIFFS at boot.
  // hasTelugu(): detects Telugu UTF-8 bytes (U+0C00–U+0C7F = 0xE0 0xB0/0xB1 xx).
  static bool g_smooth_font_loaded = false;
  static bool hasTelugu(const char* s) {
    if (!s) return false;
    while (s[0]) {
      if ((uint8_t)s[0] == 0xE0 &&
          ((uint8_t)s[1] == 0xB0 || (uint8_t)s[1] == 0xB1)) return true;
      s++;
    }
    return false;
  }

  static const int MAX_MSGS = 10;
  struct Msg { bool is_ai; char text[Config::MAX_MSG_LEN]; };
  static Msg msgs[MAX_MSGS];
  static int msg_count = 0;

  static int  str_y           = CHAT_TOP + 3;
  static int  str_x           = TEXT_X;
  static int  str_msg_start_y = 0;
  static int  str_first       = 0;
  static bool str_active      = false;
  static char str_accum[Config::MAX_MSG_LEN];
  static int  str_accum_len   = 0;

  struct Pos { int x, y; };

  static Pos layoutMsg(int sy, const Msg& m, bool render, int* ny) {
    // ── Clamp start Y into valid chat area ──
    int y = (sy < CHAT_TOP) ? CHAT_TOP : sy;
    int x = TEXT_X;
    bool is_tel = g_smooth_font_loaded && hasTelugu(m.text);

    if (render) {
      // Label is always bitmap font-2 (Latin only)
      tft.setTextFont(2); tft.setTextSize(1);
      tft.setTextColor(m.is_ai ? Pal::LABEL_AI : Pal::LABEL_USER, Pal::BG);
      tft.setCursor(3, y);
      tft.print(m.is_ai ? "AI" : "YOU");
      tft.setTextColor(m.is_ai ? Pal::TEXT_AI : Pal::TEXT_USER, Pal::BG);
    } else {
      if (!is_tel) { tft.setTextFont(2); tft.setTextSize(1); }
    }

    // ── Word buffer (up to 90 bytes to handle multi-byte UTF-8) ──
    char word[92]; int wp = 0;

    auto flush = [&]() {
      if (!wp) return;
      word[wp] = '\0';
      // Re-set font for measurement after label used font-2
      if (is_tel) {
        // smooth font textWidth works when no bitmap font override exists;
        // use a reasonable per-codepoint estimate (18px) as safe fallback.
        // Count UTF-8 codepoints in word:
        int cps = 0;
        for (int i = 0; i < wp; ) {
          uint8_t c = (uint8_t)word[i];
          if (c < 0x80) { i++; } else if (c < 0xE0) { i += 2; } else if (c < 0xF0) { i += 3; } else { i += 4; }
          cps++;
        }
        int ww = cps * 18;  // ~18px per Telugu glyph at 16pt
        int sw = 10;        // ~10px space
        if (x + ww > TEXT_X + TEXT_MAX && x > TEXT_X) { y += LINE_H; x = TEXT_X; }
        if (y + LINE_H > CHAT_BOT) { wp = 0; return; }  // !! Y-clamp
        if (render) { tft.drawString(word, x, y); }
        x += ww + sw;
      } else {
        tft.setTextFont(2); tft.setTextSize(1);
        int ww = tft.textWidth(word);
        int sw = tft.textWidth(" ");
        if (x + ww > TEXT_X + TEXT_MAX && x > TEXT_X) { y += LINE_H; x = TEXT_X; }
        if (y + LINE_H > CHAT_BOT) { wp = 0; return; }  // !! Y-clamp
        if (render) { tft.setCursor(x, y); tft.print(word); tft.print(' '); }
        x += ww + sw;
      }
      wp = 0;
    };

    const char* p = m.text;
    while (*p) {
      if (*p == ' ') {
        flush();
        p++;
      } else if (*p == '\n') {
        flush(); y += LINE_H; x = TEXT_X;
        p++;
      } else {
        // Copy full UTF-8 sequence into word buffer
        uint8_t c = (uint8_t)*p;
        int seq = (c < 0x80) ? 1 : (c < 0xE0) ? 2 : (c < 0xF0) ? 3 : 4;
        if (wp + seq < 90) {
          for (int s = 0; s < seq && *p; s++) word[wp++] = *p++;
        } else {
          p += seq; // skip overflow
        }
      }
    }
    flush();
    if (ny) *ny = y + LINE_H + MSG_GAP;
    return {x, y};
  }

  static int findFirst(int limit) {
    if (limit <= 0) return 0;
    for (int f = 0; f < limit; f++) {
      int y = CHAT_TOP + 3; bool fits = true;
      for (int i = f; i < limit; i++) {
        int ny; layoutMsg(y, msgs[i], false, &ny); y = ny;
        if (y > CHAT_BOT) { fits = false; break; }
      }
      if (fits) return f;
    }
    // Even the last message alone might overflow; return it anyway
    // renderSlice's Y-clamp will prevent drawing outside chat area.
    return limit - 1;
  }

  static int renderSlice(int first, int limit) {
    if (first < 0) first = 0;
    tft.fillRect(0, CHAT_TOP, W, CHAT_H + 1, Pal::BG);
    int y = CHAT_TOP + 3;
    for (int i = first; i < limit; i++) {
      if (y + LINE_H > CHAT_BOT) break;  // !! stop before overwriting status bar
      if (i > first)
        tft.drawFastHLine(4, y - (MSG_GAP / 2 + 1), W - 8, Pal::DIVIDER);
      int ny; layoutMsg(y, msgs[i], true, &ny); y = ny;
    }
    return y;
  }

  static void redrawAll() {
    int f = findFirst(msg_count);
    renderSlice(f, msg_count);
  }

  static void addMessage(bool is_ai, const char* text) {
    if (!text || text[0] == '\0') return;  // guard empty
    if (msg_count >= MAX_MSGS) {
      memmove(&msgs[0], &msgs[1], sizeof(Msg) * (MAX_MSGS - 1));
      msg_count = MAX_MSGS - 1;
    }
    msgs[msg_count].is_ai = is_ai;
    strlcpy(msgs[msg_count].text, text, Config::MAX_MSG_LEN);
    msg_count++;
    redrawAll();
  }

  static void streamBegin() {
    if (msg_count >= MAX_MSGS) {
      memmove(&msgs[0], &msgs[1], sizeof(Msg) * (MAX_MSGS - 1));
      msg_count = MAX_MSGS - 1;
    }
    msgs[msg_count].is_ai = true;
    msgs[msg_count].text[0] = '\0';
    msg_count++;

    str_accum_len = 0;
    str_accum[0]  = '\0';
    str_active    = true;

    // Telugu mode: accumulate silently, render at streamEnd
    if (g_is_telugu) {
      str_x = TEXT_X; str_y = CHAT_TOP + 3;
      return;  // no per-token display for Telugu
    }

    // English: set up live streaming position
    str_first = findFirst(msg_count - 1);
    int y = renderSlice(str_first, msg_count - 1);
    if (y + LINE_H > CHAT_BOT) y = CHAT_BOT - LINE_H;  // clamp

    if (msg_count - 1 > str_first)
      tft.drawFastHLine(4, y - (MSG_GAP / 2 + 1), W - 8, Pal::DIVIDER);

    str_msg_start_y = y;
    tft.setTextFont(2); tft.setTextSize(1);
    tft.setTextColor(Pal::LABEL_AI, Pal::BG);
    if (y >= CHAT_TOP && y + LINE_H <= CHAT_BOT) {
      tft.setCursor(3, y); tft.print("AI");
    }
    tft.setTextColor(Pal::TEXT_AI, Pal::BG);

    str_x = TEXT_X; str_y = y;
    tft.setCursor(str_x, str_y);
  }

  static void streamScroll() {
    // !! Commit current accumulation BEFORE findFirst re-layouts the message
    if (msg_count > 0)
      strlcpy(msgs[msg_count - 1].text, str_accum, Config::MAX_MSG_LEN);

    str_first = findFirst(msg_count);
    if (str_first < 0) str_first = 0;
    if (str_first >= msg_count) str_first = (msg_count > 0) ? msg_count - 1 : 0;

    tft.fillRect(0, CHAT_TOP, W, CHAT_H + 1, Pal::BG);
    int y = CHAT_TOP + 3;
    for (int i = str_first; i < msg_count; i++) {
      if (y + LINE_H > CHAT_BOT) break;  // !! Y-clamp
      if (i > str_first)
        tft.drawFastHLine(4, y - (MSG_GAP / 2 + 1), W - 8, Pal::DIVIDER);
      int ny;
      Pos ep = layoutMsg(y, msgs[i], true, &ny);
      if (i == msg_count - 1) {
        str_x = ep.x; str_y = ep.y;
        // Clamp final stream position
        if (str_y + LINE_H > CHAT_BOT) str_y = CHAT_BOT - LINE_H;
        if (str_x < TEXT_X) str_x = TEXT_X;
      }
      y = ny;
    }

    tft.setTextFont(2); tft.setTextSize(1);
    tft.setTextColor(Pal::TEXT_AI, Pal::BG);
    tft.setCursor(str_x, str_y);
  }

  static void streamToken(const char* token) {
    if (!str_active) return;
    if (!token || token[0] == '\0') return;

    // ── Telugu mode: just buffer, no per-token screen write ──
    if (g_is_telugu) {
      size_t tlen = strlen(token);
      if (str_accum_len + (int)tlen < Config::MAX_MSG_LEN - 2) {
        memcpy(str_accum + str_accum_len, token, tlen);
        str_accum_len += (int)tlen;
        str_accum[str_accum_len] = '\0';
      }
      return;
    }

    // ── English: typewriter rendering ──
    tft.setTextFont(2); tft.setTextSize(1);
    tft.setTextColor(Pal::TEXT_AI, Pal::BG);

    // Guard: if cursor somehow escaped chat area, recover via scroll
    if (str_y + LINE_H > CHAT_BOT) { streamScroll(); }
    if (str_y < CHAT_TOP) { str_y = CHAT_TOP; str_x = TEXT_X; }

    const char* p = token;
    while (*p) {
      if (*p == '\n') {
        if (str_accum_len < Config::MAX_MSG_LEN - 2)
          str_accum[str_accum_len++] = '\n';
        str_accum[str_accum_len] = '\0';
        str_y += LINE_H; str_x = TEXT_X;
        if (str_y + LINE_H > CHAT_BOT) streamScroll();
        tft.setCursor(str_x, str_y);
        p++; continue;
      }

      // Build a word (Latin only — single bytes)
      char word[64]; int wl = 0;
      while (*p && *p != ' ' && *p != '\n' && wl < 62) word[wl++] = *p++;
      word[wl] = '\0';

      if (wl > 0) {
        tft.setTextFont(2); tft.setTextSize(1);
        int ww = tft.textWidth(word);

        if (str_x + ww > TEXT_X + TEXT_MAX && str_x > TEXT_X) {
          str_y += LINE_H; str_x = TEXT_X;
          if (str_y + LINE_H > CHAT_BOT) streamScroll();
        }

        // Only draw if inside chat area
        if (str_y + LINE_H <= CHAT_BOT) {
          tft.setCursor(str_x, str_y);
          for (int i = 0; i < wl; i++) {
            tft.print(word[i]);
            if (str_accum_len < Config::MAX_MSG_LEN - 2)
              str_accum[str_accum_len++] = word[i];
            delay(5);
          }
        } else {
          // Still accumulate even if we can't draw
          for (int i = 0; i < wl && str_accum_len < Config::MAX_MSG_LEN - 2; i++)
            str_accum[str_accum_len++] = word[i];
        }
        str_accum[str_accum_len] = '\0';
        str_x += ww;
      }

      if (*p == ' ') {
        if (str_y + LINE_H <= CHAT_BOT) {
          tft.setCursor(str_x, str_y);
          tft.print(' ');
        }
        str_x += tft.textWidth(" ");
        if (str_accum_len < Config::MAX_MSG_LEN - 2)
          str_accum[str_accum_len++] = ' ';
        str_accum[str_accum_len] = '\0';
        p++;
      }
    }
  }

  static void streamEnd() {
    if (!str_active) return;
    if (msg_count > 0)
      strlcpy(msgs[msg_count - 1].text, str_accum, Config::MAX_MSG_LEN);
    str_active = false;
    // Telugu: re-render the full response now that we have complete text
    if (g_is_telugu && g_smooth_font_loaded) {
      redrawAll();
    }
  }

  static void streamCancel() {
    if (!str_active) return;
    if (str_accum_len > 0 && msg_count > 0)
      strlcpy(msgs[msg_count - 1].text, str_accum, Config::MAX_MSG_LEN);
    else if (msg_count > 0 && str_accum_len == 0)
      msg_count--;  // remove empty placeholder
    str_active = false;
    str_accum_len = 0;
    str_accum[0] = '\0';
  }

  // ── sanitize(): reset stream state — call on every IDLE transition ──
  // Prevents stale str_active=true from corrupting the next interaction.
  static void sanitize() {
    if (str_active) {
      Log::warn("UI", "sanitize: str_active was true — forcing cancel");
      streamCancel();
    }
    str_x         = TEXT_X;
    str_y         = CHAT_TOP + 3;
    str_accum_len = 0;
    str_accum[0]  = '\0';
  }

  static void drawWifiIcon(bool ok) {
    constexpr int IX = 278, IY = 3, IW = 30, IH = 22;
    tft.fillRect(IX, IY, IW, IH, Pal::HEADER_BG);
    int cx = IX + IW - 5, cy = IY + IH / 2;
    if (ok) {
      Net::NetQuality q = Net::quality();
      uint16_t col = (q == Net::GOOD) ? Pal::WIFI_OK :
                     (q == Net::FAIR) ? Pal::WIFI_WEAK : Pal::WIFI_BAD;
      tft.fillCircle(cx, cy, 2, col);
      tft.drawArc(cx, cy, 6,  4, 135, 225, col, Pal::HEADER_BG);
      tft.drawArc(cx, cy, 11, 9, 135, 225, col, Pal::HEADER_BG);
    } else {
      uint16_t G = 0x4228;
      tft.fillCircle(cx, cy, 2, G);
      tft.drawArc(cx, cy, 6,  4, 135, 225, G, Pal::HEADER_BG);
      tft.drawArc(cx, cy, 11, 9, 135, 225, G, Pal::HEADER_BG);
      tft.drawLine(IX + IW - 2, IY + 2, IX + 2, IY + IH - 3, Pal::WIFI_BAD);
      tft.drawLine(IX + IW - 2, IY + 3, IX + 2, IY + IH - 2, Pal::WIFI_BAD);
    }
  }

  static void drawRSSI() {
    if (!Net::isUp()) return;
    int r    = Net::rssi();
    int bars = (r > -55) ? 4 : (r > -70) ? 3 : (r > -80) ? 2 : (r > -90) ? 1 : 0;
    int bx   = 258;
    for (int i = 0; i < 4; i++) {
      int bh = 4 + i * 3, by = 22 - bh;
      uint16_t c = (i < bars) ? Pal::WIFI_OK : (uint16_t)0x2104;
      tft.fillRect(bx + i * 5, by + 3, 3, bh, c);
    }
  }

  static void setStatus(const char* msg, uint16_t bg, uint16_t fg = TFT_WHITE) {
    tft.fillRect(0, SB_Y, W, SB_H, Pal::BG);
    tft.fillRoundRect(4, SB_Y + 2, W - 8, SB_H - 4, 4, bg);
    tft.setTextFont(2); tft.setTextSize(1);
    tft.setTextColor(fg, bg);
    int tw = tft.textWidth(msg);
    tft.setCursor((W - tw) / 2, SB_Y + 7); tft.print(msg);
  }

  static void showError(ErrSource src, const char* msg) {
    char buf[80];
    snprintf(buf, sizeof(buf), "[%s] %s", errSourceName(src), msg);
    setStatus(buf, Pal::S_ERROR);
  }

  // ── Sleep Mode — Moon & Stars ──
  struct Star { int x, y; uint8_t bright; int8_t phase; };
  static Star     stars[25];
  static uint8_t  sleep_frame   = 0;
  static uint32_t sleep_anim_ts = 0;

  static void initStars() {
    uint32_t s = 12345;
    for (int i = 0; i < 25; i++) {
      s = s * 1103515245 + 12345; stars[i].x = 10 + (s >> 16) % (W - 20);
      s = s * 1103515245 + 12345; stars[i].y = 40 + (s >> 16) % (H - 90);
      stars[i].bright = (s >> 8) & 3; stars[i].phase = i % 7;
      if (stars[i].x > 180 && stars[i].x < 280 && stars[i].y > 50 && stars[i].y < 150)
        stars[i].x = 30 + (i * 17) % 140;
    }
  }

  static void drawSleepScreen() {
    tft.fillScreen(Pal::SLEEP_BG);
    tft.fillCircle(230, 80, 28, Pal::MOON);
    tft.fillCircle(244, 71, 26, Pal::SLEEP_BG);
    tft.drawCircle(230, 80, 29, 0x4208);

    for (int i = 0; i < 25; i++) {
      uint8_t b = (stars[i].bright + sleep_frame + stars[i].phase) % 4;
      uint16_t c = (b == 0) ? 0x2104 : (b == 1) ? Pal::STAR_DIM : (b == 2) ? 0xC618 : Pal::STAR;
      tft.drawPixel(stars[i].x, stars[i].y, c);
      if (b == 3 && (i % 3 == 0)) {
        tft.drawPixel(stars[i].x - 1, stars[i].y,     0x6B4D);
        tft.drawPixel(stars[i].x + 1, stars[i].y,     0x6B4D);
        tft.drawPixel(stars[i].x,     stars[i].y - 1, 0x6B4D);
        tft.drawPixel(stars[i].x,     stars[i].y + 1, 0x6B4D);
      }
    }
    tft.setTextFont(2); tft.setTextSize(1);
    tft.setTextColor(0x4228, Pal::SLEEP_BG);
    const char* zzz = "Sleeping... press button to wake";
    tft.setCursor((W - tft.textWidth(zzz)) / 2, H - 30);
    tft.print(zzz);
  }

  static void animateSleep() {
    if (millis() - sleep_anim_ts < 800) return;
    sleep_anim_ts = millis(); sleep_frame++;

    for (int i = 0; i < 25; i++) {
      tft.drawPixel(stars[i].x, stars[i].y, Pal::SLEEP_BG);
      if (i % 3 == 0) {
        tft.drawPixel(stars[i].x - 1, stars[i].y,     Pal::SLEEP_BG);
        tft.drawPixel(stars[i].x + 1, stars[i].y,     Pal::SLEEP_BG);
        tft.drawPixel(stars[i].x,     stars[i].y - 1, Pal::SLEEP_BG);
        tft.drawPixel(stars[i].x,     stars[i].y + 1, Pal::SLEEP_BG);
      }
      uint8_t b = (stars[i].bright + sleep_frame + stars[i].phase) % 4;
      uint16_t c = (b == 0) ? 0x2104 : (b == 1) ? Pal::STAR_DIM : (b == 2) ? 0xC618 : Pal::STAR;
      tft.drawPixel(stars[i].x, stars[i].y, c);
      if (b == 3 && (i % 3 == 0)) {
        tft.drawPixel(stars[i].x - 1, stars[i].y,     0x6B4D);
        tft.drawPixel(stars[i].x + 1, stars[i].y,     0x6B4D);
        tft.drawPixel(stars[i].x,     stars[i].y - 1, 0x6B4D);
        tft.drawPixel(stars[i].x,     stars[i].y + 1, 0x6B4D);
      }
    }

    tft.fillRect(255, 20, 60, 50, Pal::SLEEP_BG);
    tft.setTextFont(2); tft.setTextColor(Pal::STAR_DIM, Pal::SLEEP_BG);
    for (int i = 0; i < 3; i++) {
      if ((sleep_frame + i) % 5 < 3) {
        tft.setCursor(260 + i * 8, 55 - i * 12); tft.print("z");
      }
    }
  }

  static void buildSkeleton() {
    tft.fillScreen(Pal::BG);
    tft.fillRect(0, 0, W, HDR_H, Pal::HEADER_BG);
    tft.drawFastHLine(0, HDR_H, W, Pal::HEADER_LINE);
    tft.setTextFont(2); tft.setTextSize(1);
    tft.setTextColor(Pal::TEXT_TITLE, Pal::HEADER_BG);
    const char* t = "AI Chatbot";
    tft.setCursor((W - tft.textWidth(t)) / 2 - 10, 6); tft.print(t);
    drawWifiIcon(false);
    msg_count = 0;
  }

  static void begin() {
    tft.init(); tft.setRotation(1); tft.fillScreen(Pal::BG);
    initStars(); buildSkeleton();
    Log::info("UI", "Display %dx%d ready", W, H);

    // ── Load Telugu smooth font from SPIFFS ──
    // Font file: data/NotoTelugu16.vlw  (see README for generation steps)
    // If not found, we fall back to Latin-only bitmap rendering.
    if (SPIFFS.begin(true)) {  // true = format if mount fails
      if (SPIFFS.exists("/NotoTelugu16.vlw")) {
        tft.loadFont("NotoTelugu16", SPIFFS);
        g_smooth_font_loaded = true;
        Log::info("UI", "Telugu smooth font loaded OK");
      } else {
        Log::warn("UI", "NotoTelugu16.vlw not found — Latin-only mode");
      }
    } else {
      Log::warn("UI", "SPIFFS mount failed — Telugu font unavailable");
    }
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 15 — OFFLINE MODE
// ══════════════════════════════════════════════════════════════
namespace Offline {
  static uint8_t resp_idx = 0;
  static const char* getResponse() {
    const char* r = Config::OFFLINE_RESPONSES[resp_idx % Config::OFFLINE_RESP_COUNT];
    resp_idx++;
    return r;
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 16 — STATE CONTROLLER
// ══════════════════════════════════════════════════════════════
static void enterState(AppState next) {
  g_prev_state = g_state;
  g_state      = next;
  g_state_ts   = millis();
  Log::state(g_prev_state, next);

  switch (next) {
    case AppState::IDLE:
      UI::sanitize();  // clear any stale stream state on every IDLE entry
      UI::setStatus("HOLD BUTTON TO SPEAK",
                    g_wifi_ok ? Pal::S_IDLE : Pal::S_OFFLINE);
      if (g_wifi_ok) UI::drawRSSI();
      g_last_activity = millis();
      break;
    case AppState::RECORDING:
      UI::setStatus("RECORDING... RELEASE TO STOP", Pal::S_RECORD);
      g_last_activity = millis();
      break;
    case AppState::TRANSCRIBING:
      UI::setStatus("TRANSCRIBING (SARVAM)...", Pal::S_PROC, TFT_BLACK);
      break;
    case AppState::RESPONDING:
      UI::setStatus("AI RESPONDING...", Pal::S_STREAM, TFT_BLACK);
      break;
    case AppState::SLEEP:
      g_sleep_drawn = false;
      Log::info("SLEEP", "Entering (idle %us)", (millis() - g_last_activity) / 1000);
      break;
    case AppState::WIFI_RETRY:
      UI::setStatus("RECONNECTING WiFi...", Pal::S_OFFLINE);
      UI::drawWifiIcon(false);
      break;
    case AppState::ERROR_DISPLAY:
      break;
    default: break;
  }
}

static void triggerError(ErrSource src, const char* msg,
                         uint16_t display_ms = 2000) {
  if (g_last_error.source == src)
    g_last_error.count++;
  else
    g_last_error = {src, "", millis(), 1};
  strlcpy(g_last_error.message, msg, sizeof(g_last_error.message));

  Log::error(errSourceName(src), "%s (x%u)", msg, g_last_error.count);

  // Cancel any active stream BEFORE showing error to prevent UI corruption
  UI::streamCancel();
  g_is_telugu = false;  // reset language flag on error

  enterState(AppState::ERROR_DISPLAY);
  UI::showError(src, msg);
  g_error_until = millis() + display_ms;
}

// ══════════════════════════════════════════════════════════════
//  SECTION 17 — SETUP
// ══════════════════════════════════════════════════════════════
void setup() {
  Serial.begin(115200);
  delay(100);

  Serial.println();
  Serial.println(F("╔════════════════════════════════════════════════════╗"));
  Serial.println(F("║  ESP32 AI Voice Assistant v4.1 — Sarvam AI STT    ║"));
  Serial.println(F("║  STT: Sarvam Saaras v3 | LLM: Cerebras           ║"));
  Serial.println(F("╚════════════════════════════════════════════════════╝"));
  Log::heap("Boot");

  Health::init();
  pinMode(Config::BUTTON_PIN, INPUT_PULLUP);

  // Allocate audio buffer FIRST — grab large contiguous block
  // before TFT DMA init can fragment heap
  if (!Audio::allocate()) {
    Serial.println(F("FATAL: Audio buffer alloc failed"));
    while (true) delay(1000);
  }

  // Display init (after audio alloc)
  UI::begin();
  UI::setStatus("INITIALIZING...", Pal::S_PROC, TFT_BLACK);

  // I2S microphone
  if (!Audio::begin()) {
    UI::setStatus("FATAL: MIC INIT FAIL", Pal::S_ERROR);
    Log::fatal("BOOT", "I2S init failed");
    while (true) delay(1000);
  }

  // WiFi
  UI::setStatus("CONNECTING WiFi...", Pal::S_PROC, TFT_BLACK);
  g_wifi_ok = Net::connect();
  UI::drawWifiIcon(g_wifi_ok);

  if (g_wifi_ok) {
    UI::drawRSSI();
    Log::info("BOOT", "WiFi OK: %s RSSI:%d",
              WiFi.localIP().toString().c_str(), WiFi.RSSI());
  } else {
    Log::warn("BOOT", "WiFi failed — will retry");
    g_wifi_retry_ts = millis();
  }

  Log::heap("Boot done");
  enterState(AppState::IDLE);
  Log::info("BOOT", "════ System Ready (Sarvam AI STT) ════");
}

// ══════════════════════════════════════════════════════════════
//  SECTION 18 — MAIN LOOP
// ══════════════════════════════════════════════════════════════
void loop() {

  // Health monitor — runs every 5 seconds
  Health::check();

  // ──────────────────────────────────────────────────────────
  //  ERROR_DISPLAY — timed auto-recovery
  // ──────────────────────────────────────────────────────────
  if (g_state == AppState::ERROR_DISPLAY) {
    if (millis() >= g_error_until) {
      enterState(AppState::IDLE);
    }
    if (digitalRead(Config::BUTTON_PIN) == LOW) {
      delay(Config::DEBOUNCE_MS);
      if (digitalRead(Config::BUTTON_PIN) == LOW) {
        while (digitalRead(Config::BUTTON_PIN) == LOW) delay(20);
        enterState(AppState::IDLE);
      }
    }
    delay(50);
    return;
  }

  // ──────────────────────────────────────────────────────────
  //  WiFi Watchdog (skipped during SLEEP)
  // ──────────────────────────────────────────────────────────
  if (g_state != AppState::SLEEP) {
    bool currently_up = Net::isUp();

    if (!currently_up && g_wifi_ok) {
      g_wifi_ok = false;
      UI::drawWifiIcon(false);
      g_wifi_retry_ts = millis();
      Log::warn("WIFI", "Connection lost");
    }

    if (!currently_up) {
      if (g_state == AppState::IDLE || g_state == AppState::WIFI_RETRY) {
        if (g_state != AppState::WIFI_RETRY) enterState(AppState::WIFI_RETRY);

        if (millis() - g_wifi_retry_ts > Config::WIFI_RETRY_MS) {
          Log::info("WIFI", "Attempting reconnect...");
          g_wifi_ok = Net::connect();
          UI::drawWifiIcon(g_wifi_ok);
          g_wifi_retry_ts = millis();
          if (g_wifi_ok) { UI::drawRSSI(); enterState(AppState::IDLE); }
        }

        if (digitalRead(Config::BUTTON_PIN) == LOW) {
          delay(Config::DEBOUNCE_MS);
          if (digitalRead(Config::BUTTON_PIN) == LOW) {
            g_wifi_ok = Net::connect();
            UI::drawWifiIcon(g_wifi_ok);
            if (g_wifi_ok) { UI::drawRSSI(); enterState(AppState::IDLE); }
            while (digitalRead(Config::BUTTON_PIN) == LOW) delay(20);
          }
        }

        if (!g_wifi_ok) { delay(50); return; }
      }
    } else if (!g_wifi_ok) {
      g_wifi_ok = true;
      UI::drawWifiIcon(true);
      UI::drawRSSI();
      Log::info("WIFI", "Reconnected RSSI:%d", Net::rssi());
      if (g_state == AppState::WIFI_RETRY) enterState(AppState::IDLE);
    }
  }

  // ──────────────────────────────────────────────────────────
  //  SLEEP state
  // ──────────────────────────────────────────────────────────
  if (g_state == AppState::SLEEP) {
    if (!g_sleep_drawn) {
      UI::drawSleepScreen();
      g_sleep_drawn = true;
      Net::enableModemSleep();
    }
    UI::animateSleep();

    if (digitalRead(Config::BUTTON_PIN) == LOW) {
      delay(Config::DEBOUNCE_MS);
      if (digitalRead(Config::BUTTON_PIN) == LOW) {
        Log::info("SLEEP", "Waking up");
        Net::disableModemSleep();
        g_wifi_ok = Net::isUp();
        if (!g_wifi_ok) {
          UI::buildSkeleton();
          UI::setStatus("RECONNECTING...", Pal::S_PROC, TFT_BLACK);
          g_wifi_ok = Net::connect();
        }
        UI::buildSkeleton();
        UI::drawWifiIcon(g_wifi_ok);
        if (g_wifi_ok) UI::drawRSSI();
        UI::redrawAll();
        enterState(AppState::IDLE);
        while (digitalRead(Config::BUTTON_PIN) == LOW) delay(20);
      }
    }
    delay(80);
    return;
  }

  // ──────────────────────────────────────────────────────────
  //  Sleep timeout
  // ──────────────────────────────────────────────────────────
  if (g_state == AppState::IDLE &&
      millis() - g_last_activity > Config::SLEEP_TIMEOUT_MS) {
    enterState(AppState::SLEEP);
    return;
  }

  // ──────────────────────────────────────────────────────────
  //  IDLE — await button press
  // ──────────────────────────────────────────────────────────
  if (g_state == AppState::IDLE) {
    if (digitalRead(Config::BUTTON_PIN) == LOW) {
      delay(Config::DEBOUNCE_MS);
      if (digitalRead(Config::BUTTON_PIN) == LOW) {
        enterState(AppState::RECORDING);
      }
    }
    delay(15);
    return;
  }

  // ──────────────────────────────────────────────────────────
  //  RECORDING → TRANSCRIBING → RESPONDING pipeline
  // ──────────────────────────────────────────────────────────
  if (g_state == AppState::RECORDING) {
    g_pipeline_start = millis();
    g_is_telugu = false;           // will be set after STT if Telugu detected
    strlcpy(g_detected_lang, "en-IN", sizeof(g_detected_lang));  // reset lang

    // Record audio
    Audio::record();
    while (digitalRead(Config::BUTTON_PIN) == LOW) delay(20);
    uint32_t rec_ms = Audio::rec_duration_ms;

    if (!Audio::ready || Audio::pcm_size < Config::MIN_AUDIO_BYTES) {
      triggerError(ErrSource::AUDIO, "TOO SHORT", 1500);
      return;
    }

    // Offline fallback
    if (!Net::isUp()) {
      Log::info("OFFLINE", "No WiFi — using offline response");
      UI::addMessage(false, "(offline recording)");
      UI::addMessage(true, Offline::getResponse());
      UI::setStatus("OFFLINE MODE", Pal::S_OFFLINE);
      delay(1000);
      enterState(AppState::IDLE);
      return;
    }

    // ── TRANSCRIBE via Sarvam AI ──
    enterState(AppState::TRANSCRIBING);
    char     transcript[Config::MAX_MSG_LEN];
    uint32_t stt_ms = 0;

    if (!STT::transcribe(transcript, sizeof(transcript), &stt_ms)) {
      triggerError(ErrSource::STT, "NOT UNDERSTOOD", 1800);
      return;
    }

    // Determine language from Sarvam response — governs display + LLM mode
    g_is_telugu = (g_detected_lang[0] == 't' && g_detected_lang[1] == 'e');
    Log::info("PIPE", "Lang:%s Telugu:%s", g_detected_lang, g_is_telugu ? "YES" : "no");

    UI::addMessage(false, transcript);
    Memory::add("user", transcript);

    // Pipeline watchdog check before LLM
    if (millis() - g_pipeline_start > Config::PIPELINE_MAX_MS) {
      triggerError(ErrSource::SYSTEM, "PIPELINE TIMEOUT", 1500);
      return;
    }

    // ── LLM RESPONSE (Dual Mode: stream + fallback) ──
    enterState(AppState::RESPONDING);

    char     ai_response[Config::MAX_MSG_LEN];
    uint32_t llm_ms    = 0;
    bool     use_stream = (Net::quality() != Net::POOR);
    bool     llm_ok     = false;

    if (use_stream) {
      TokenQ::reset();
      UI::streamBegin();

      llm_ok = LLM::streamRequest(
        transcript, ai_response, sizeof(ai_response), &llm_ms);

      // Drain any tokens still in queue
      char tok[Config::TOKEN_MAX_LEN];
      while (TokenQ::pop(tok, sizeof(tok))) UI::streamToken(tok);

      if (llm_ok) {
        UI::streamEnd();
      } else {
        UI::streamCancel();
        Log::warn("LLM", "Stream failed — trying fallback");
        Net::killTLS();
        delay(200);
        UI::setStatus("RETRYING...", Pal::S_WARN, TFT_BLACK);

        llm_ok = LLM::fallbackRequest(
          transcript, ai_response, sizeof(ai_response), &llm_ms);

        if (llm_ok) {
          UI::streamBegin();
          const char* p = ai_response;
          char word[64]; int wl = 0;
          while (*p) {
            if (*p == ' ' || *p == '\n') {
              if (wl > 0) {
                word[wl] = '\0';
                UI::streamToken(word);
                UI::streamToken(" ");
                wl = 0;
              }
              if (*p == '\n') UI::streamToken("\n");
              p++;
            } else {
              if (wl < 62) word[wl++] = *p;
              p++;
            }
          }
          if (wl > 0) { word[wl] = '\0'; UI::streamToken(word); }
          UI::streamEnd();
        }
      }
    } else {
      // Direct fallback path (poor signal)
      llm_ok = LLM::fallbackRequest(
        transcript, ai_response, sizeof(ai_response), &llm_ms);

      if (llm_ok) {
        UI::streamBegin();
        const char* p = ai_response;
        char word[64]; int wl = 0;
        while (*p) {
          if (*p == ' ' || *p == '\n') {
            if (wl > 0) {
              word[wl] = '\0';
              UI::streamToken(word);
              UI::streamToken(" ");
              wl = 0;
            }
            if (*p == '\n') UI::streamToken("\n");
            p++;
          } else {
            if (wl < 62) word[wl++] = *p;
            p++;
          }
        }
        if (wl > 0) { word[wl] = '\0'; UI::streamToken(word); }
        UI::streamEnd();
      }
    }

    if (!llm_ok) {
      triggerError(ErrSource::LLM, "NO AI RESPONSE", 1800);
      return;
    }

    Memory::add("assistant", ai_response);

    uint32_t total_ms = millis() - g_pipeline_start;
    Log::pipeline(rec_ms, stt_ms, llm_ms, total_ms, LLM::g_streamed, transcript);
    Log::heap("Pipeline done");

    enterState(AppState::IDLE);
    return;
  }

  delay(15);
}