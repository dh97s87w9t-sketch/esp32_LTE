#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <esp_task_wdt.h>
#include <Preferences.h>
#include "esp_system.h"

extern "C" {
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
}

#include "secrets.h"

#define VERSION "2.7.0-prod"

#define WDT_TIMEOUT 60000
#define CPU_FREQ_MHZ 80   // ESP32 z WiFi: dozwolone tylko 80 / 160 / 240

#define USE_STATIC_IP true
#define STATIC_IP "192.168.10.87"
#define GATEWAY "192.168.10.1"
#define SUBNET "255.255.255.0"
#define DNS_PRIMARY "1.1.1.2"
#define DNS_SECONDARY "1.1.1.1"

#define WIFI_CONNECT_TIMEOUT_MS 15000
#define WIFI_RECONNECT_INTERVAL 30000
#define WIFI_RESTART_AFTER_MS 600000   // restart ESP dopiero po 10 min bez WiFi

#define MQTT_BROKER "192.168.10.88"
#define MQTT_PORT 1883
#define MQTT_BUFFER_SIZE 768
#define MQTT_MAX_INCOMING_PAYLOAD 700
#define MQTT_KEEPALIVE_SEC 60
#define MQTT_SOCKET_TIMEOUT_SEC 10
#define MQTT_RECONNECT_INTERVAL 5000

#define DEVICE_ID "esp32_sms_gateway"

#define MQTT_TOPIC_SEND "homeassistant/sms/send"
#define MQTT_TOPIC_RECEIVED "homeassistant/sms/received"
#define MQTT_TOPIC_STATUS "homeassistant/sms/status"
#define MQTT_TOPIC_DISCOVERY "homeassistant/notify/esp32_sms/config"
#define MQTT_TOPIC_DIAG "homeassistant/sms/diag"

#define MODEM_TX 16
#define MODEM_RX 17
#define MODEM_PWRKEY 15
// Jeśli masz podłączony RESET_N modemu, ustaw pin; jak nie masz => -1
#define MODEM_RESET_N -1   // np. 5 jeśli masz podpięte; -1 = brak
// Jeśli masz podłączony pin STATUS modemu (HIGH = włączony), ustaw pin; jak nie masz => -1.
// Pozwala uniknąć wyłączenia działającego modemu kolejnym impulsem PWRKEY.
#define MODEM_STATUS_PIN -1

#define MODEM_CHECK_INTERVAL 60000
#define MODEM_REINIT_INTERVAL 60000   // ponowna próba inicjalizacji, gdy modem nie działa
#define HEAP_CHECK_INTERVAL 30000
#define SMS_SEND_DELAY 3000
#define SIGNAL_CHECK_INTERVAL 120000
#define SIGNAL_RECHECK_BLOCKED_MS 15000   // częstsze odświeżanie CSQ, gdy wysyłka czeka na sygnał

#define SMS_QUEUE_SIZE 20
#define SMS_MAX_RETRIES 2          // ponowienia po pierwszej próbie (łącznie 3 próby)
#define SMS_RETRY_DELAY_MS 10000
#define SMS_MAX_LEN 160            // tryb tekstowy GSM 7-bit: jedna wiadomość

#define AT_COMMAND_TIMEOUT 5000
#define SMS_PROMPT_TIMEOUT 6000
#define SMS_SEND_TIMEOUT 30000
#define SMS_COOLDOWN_MS 800

#define SIGNAL_THRESHOLD 10

#define SMS_DEDUP_TIME 60000
#define SMS_CACHE_SIZE 10
#define INCOMING_SMS_DEDUP_TIME 120000
#define INCOMING_SMS_CACHE_SIZE 8

#define TX_STUCK_RESET_MS 180000
#define TX_BLOCK_LOG_MS 30000

#define SMS_STORAGE_CLEAN_ON_BOOT true

// ---- modem cold-boot tuning ----
#define MODEM_BOOT_PROBE_MS 12000
#define MODEM_PWRKEY_PULSE_MS 1600
#define MODEM_AFTER_PULSE_WAIT_MS 5000
#define MODEM_AFTER_PULSE_PROBE_MS 20000   // długie okno, by nie "wyłączyć" wolno startującego modemu kolejnym impulsem
#define MODEM_POWERON_TRIES 3
#define MODEM_CRESET_WAIT_MS 15000
#define MODEM_SIM_READY_TRIES 5

#define MODEM_LINE_MAX 256

#define NVS_NAMESPACE "sms-gateway"

// Numery uprawnione do wysyłania SMS-ów do bramki (bez '+').
// Można nadpisać w secrets.h:  #define SMS_ALLOWED_NUMBERS "48600000000", "48700000000"
#ifndef SMS_ALLOWED_NUMBERS
#define SMS_ALLOWED_NUMBERS "48608057658", "48691029627"
#endif
static const char* const ALLOWED_NUMBERS[] = { SMS_ALLOWED_NUMBERS };
static const size_t ALLOWED_NUMBERS_COUNT = sizeof(ALLOWED_NUMBERS) / sizeof(ALLOWED_NUMBERS[0]);

struct SMSMessage { String phone; String message; bool pending = false; uint8_t retries = 0; uint32_t seq = 0; unsigned long lastAttempt = 0; };
struct SMSCache { String phone; String message; unsigned long timestamp; bool active; };
struct IncomingSMSCache { String sender; String message; unsigned long timestamp; bool active; };

enum AtResult { AT_OK, AT_ERROR, AT_TIMEOUT };
enum SendResult { SEND_OK, SEND_FAIL, SEND_UNCERTAIN };

// ArduinoJson 6 i 7: w v7 StaticJsonDocument jest przestarzały (dokument rośnie dynamicznie).
#if ARDUINOJSON_VERSION_MAJOR >= 7
template <size_t N> using JsonDoc = JsonDocument;
#else
template <size_t N> using JsonDoc = StaticJsonDocument<N>;
#endif

#define LOG(msg) Serial.println(msg)

HardwareSerial SerialAT(1);
WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
Preferences preferences;

unsigned long totalResets = 0;

unsigned long smsSentSuccess = 0;
unsigned long smsSentFailed = 0;
unsigned long smsSentUncertain = 0;
unsigned long smsDroppedFromNVS = 0;

unsigned long smsReceived = 0;
unsigned long smsCommandsProcessed = 0;
unsigned long unauthorizedAttempts = 0;
unsigned long smsDuplicatesBlocked = 0;
unsigned long incomingDuplicatesBlocked = 0;

bool modemReady = false;
bool modemNeedsInit = false;
bool modemInitInProgress = false;
bool watchdogEnabled = false;
bool statusDirty = false;
bool bootDiagSent = false;
bool txBlockedLogged = false;
String currentOperator = "";
int signalStrength = 99;
int signalQuality = -999;

unsigned long lastMQTTReconnect = 0;
unsigned long lastSMSSent = 0;
unsigned long lastModemCheck = 0;
unsigned long lastModemInitAttempt = 0;
unsigned long lastHeapCheck = 0;
unsigned long lastSignalCheck = 0;

unsigned long lastTxProgressMs = 0;
unsigned long lastTxBlockLogMs = 0;

bool wifiDown = false;
unsigned long wifiDownSince = 0;
unsigned long lastWifiReconnect = 0;

SMSMessage smsQueue[SMS_QUEUE_SIZE];
int pendingCount = 0;
uint32_t nextSeq = 1;

SMSCache smsCache[SMS_CACHE_SIZE];
int smsCacheIndex = 0;

IncomingSMSCache incomingSMSCache[INCOMING_SMS_CACHE_SIZE];
int incomingSMSCacheIndex = 0;

esp_reset_reason_t bootResetReason = ESP_RST_UNKNOWN;
String lastAction = "";

static inline bool timePassed(unsigned long now, unsigned long then, unsigned long ms) {
  return (long)(now - then) >= (long)ms;
}
static inline unsigned long elapsedMs(unsigned long now, unsigned long then) {
  return (unsigned long)(now - then);
}

void feedWatchdog() { if (watchdogEnabled) esp_task_wdt_reset(); }

// Wywoływane w każdej dłuższej pętli oczekiwania: karmi watchdoga i utrzymuje MQTT przy życiu.
void serviceBackground() {
  static bool inService = false;
  feedWatchdog();
  if (inService) return;
  inService = true;
  if (mqttClient.connected()) mqttClient.loop();
  inService = false;
}

void waitMs(unsigned long ms) {
  unsigned long start = millis();
  while (!timePassed(millis(), start, ms)) { serviceBackground(); delay(10); }
}

static inline uint32_t getStackHighWaterMarkBytes() {
#if (INCLUDE_uxTaskGetStackHighWaterMark == 1)
  UBaseType_t words = uxTaskGetStackHighWaterMark(NULL);
  return (uint32_t)words * 4;
#else
  return 0;
#endif
}

void publishDiag(const char *type, const String &msg) {
  if (!mqttClient.connected()) return;
  JsonDoc<512> doc;
  doc["type"] = type;
  doc["msg"] = msg.length() > 300 ? msg.substring(0, 300) : msg;
  doc["uptime"] = millis() / 1000;
  doc["heap"] = ESP.getFreeHeap();
  doc["min_heap"] = ESP.getMinFreeHeap();
  doc["stack_hwm_bytes"] = getStackHighWaterMarkBytes();
  char buf[512];
  size_t n = serializeJson(doc, buf, sizeof(buf));
  mqttClient.publish(MQTT_TOPIC_DIAG, (uint8_t*)buf, n, false);
}

static const char* resetReasonToStr(esp_reset_reason_t r) {
  switch (r) {
    case ESP_RST_UNKNOWN:    return "UNKNOWN";
    case ESP_RST_POWERON:    return "POWERON";
    case ESP_RST_EXT:        return "EXT";
    case ESP_RST_SW:         return "SW";
    case ESP_RST_PANIC:      return "PANIC";
    case ESP_RST_INT_WDT:    return "INT_WDT";
    case ESP_RST_TASK_WDT:   return "TASK_WDT";
    case ESP_RST_WDT:        return "WDT";
    case ESP_RST_DEEPSLEEP:  return "DEEPSLEEP";
    case ESP_RST_BROWNOUT:   return "BROWNOUT";
    case ESP_RST_SDIO:       return "SDIO";
    default:                 return "OTHER";
  }
}

// Zapamiętuje powód celowego restartu - raportowany po starcie w diag "boot".
void rememberAction(const char *action) {
  preferences.begin(NVS_NAMESPACE, false);
  preferences.putString("lastAction", action);
  preferences.end();
}

String readAndClearLastAction() {
  preferences.begin(NVS_NAMESPACE, false);
  String a;
  if (preferences.isKey("lastAction")) {
    a = preferences.getString("lastAction", "");
    preferences.remove("lastAction");
  }
  preferences.end();
  return a;
}

void printResetReason() {
  bootResetReason = esp_reset_reason();
  lastAction = readAndClearLastAction();

  LOG(String("Reset reason: ") + resetReasonToStr(bootResetReason) + " (" + String((int)bootResetReason) + ")");
  if (lastAction.length() > 0) LOG("Last action before reset: " + lastAction);

  preferences.begin(NVS_NAMESPACE, false);
  totalResets = preferences.getUInt("resetCount", 0) + 1;
  preferences.putUInt("resetCount", totalResets);
  preferences.putInt("lastReset", (int)bootResetReason);
  preferences.end();

  LOG("Total resets: " + String(totalResets));
  LOG("Boot diag: heap=" + String(ESP.getFreeHeap()) +
      " min_heap=" + String(ESP.getMinFreeHeap()) +
      " stack_hwm_bytes=" + String(getStackHighWaterMarkBytes()));
}

// ---------------- Walidacja danych wejściowych ----------------
// Dopuszcza tylko cyfry z opcjonalnym '+' na początku (spacje, '-', nawiasy są usuwane).
// Chroni przed wstrzyknięciem komend AT przez numer telefonu.
bool normalizePhone(const String &in, String &out) {
  out = "";
  size_t digits = 0;
  for (size_t i = 0; i < in.length(); i++) {
    char c = in[i];
    if (c >= '0' && c <= '9') { out += c; digits++; }
    else if (c == '+' && out.length() == 0) out += c;
    else if (c == ' ' || c == '-' || c == '(' || c == ')') continue;
    else return false;
  }
  return digits >= 3 && digits <= 20;
}

static char transliterate(uint32_t cp) {
  static const struct { uint16_t cp; char ascii; } map[] = {
    {0x104, 'A'}, {0x105, 'a'}, {0x106, 'C'}, {0x107, 'c'}, {0x118, 'E'}, {0x119, 'e'},
    {0x141, 'L'}, {0x142, 'l'}, {0x143, 'N'}, {0x144, 'n'}, {0x0D3, 'O'}, {0x0F3, 'o'},
    {0x15A, 'S'}, {0x15B, 's'}, {0x179, 'Z'}, {0x17A, 'z'}, {0x17B, 'Z'}, {0x17C, 'z'},
  };
  for (const auto &m : map) if (m.cp == cp) return m.ascii;
  return '?';
}

// Przygotowuje treść do trybu tekstowego GSM: usuwa znaki sterujące (m.in. Ctrl-Z i ESC,
// które przerwałyby AT+CMGS), zamienia polskie znaki UTF-8 na ASCII i przycina do SMS_MAX_LEN.
String sanitizeSmsText(const String &in, bool &truncated) {
  String out;
  out.reserve(in.length() < SMS_MAX_LEN ? in.length() : SMS_MAX_LEN);
  truncated = false;
  const size_t n = in.length();
  for (size_t i = 0; i < n;) {
    uint8_t c = (uint8_t)in[i];
    uint32_t cp;
    size_t len;
    if (c < 0x80) { cp = c; len = 1; }
    else if ((c & 0xE0) == 0xC0 && i + 1 < n) { cp = ((uint32_t)(c & 0x1F) << 6) | ((uint8_t)in[i + 1] & 0x3F); len = 2; }
    else if ((c & 0xF0) == 0xE0 && i + 2 < n) { cp = 0xFFFD; len = 3; }
    else if ((c & 0xF8) == 0xF0 && i + 3 < n) { cp = 0xFFFD; len = 4; }
    else { cp = 0xFFFD; len = 1; }
    i += len;

    char outc;
    if (cp == '\n') outc = '\n';
    else if (cp == '\t') outc = ' ';
    else if (cp < 0x20 || cp == 0x7F) continue;
    else if (cp < 0x80) outc = (char)cp;
    else outc = transliterate(cp);

    if (out.length() >= SMS_MAX_LEN) { truncated = true; break; }
    out += outc;
  }
  out.trim();
  return out;
}

// ---------------- Queue ----------------
void recountPending() {
  int c = 0;
  for (int i = 0; i < SMS_QUEUE_SIZE; i++) if (smsQueue[i].pending) c++;
  pendingCount = c;
}

// Najstarsza oczekująca wiadomość (FIFO wg numeru sekwencyjnego).
int findNextPending() {
  int best = -1;
  for (int i = 0; i < SMS_QUEUE_SIZE; i++)
    if (smsQueue[i].pending && (best < 0 || smsQueue[i].seq < smsQueue[best].seq)) best = i;
  return best;
}

// Najstarsza wiadomość, dla której minął już odstęp między próbami.
int findNextSendable(unsigned long now) {
  int best = -1;
  for (int i = 0; i < SMS_QUEUE_SIZE; i++) {
    const SMSMessage &m = smsQueue[i];
    if (!m.pending) continue;
    if (m.lastAttempt != 0 && !timePassed(now, m.lastAttempt, SMS_RETRY_DELAY_MS)) continue;
    if (best < 0 || m.seq < smsQueue[best].seq) best = i;
  }
  return best;
}

int findFreeSlot() { for (int i = 0; i < SMS_QUEUE_SIZE; i++) if (!smsQueue[i].pending) return i; return -1; }

// ---------------- Queue persistence ----------------
// Format v2 (klucz "q<i>"): seq|retries|phone|message - treść na końcu, więc może zawierać '|'.
// Zapisywany jest tylko zmieniony slot (mniej zapisów do flash).
void saveQueueSlot(int i) {
  char key[12];
  snprintf(key, sizeof(key), "q%d", i);
  preferences.begin(NVS_NAMESPACE, false);
  if (smsQueue[i].pending) {
    String data = String(smsQueue[i].seq) + "|" + String(smsQueue[i].retries) + "|" +
                  smsQueue[i].phone + "|" + smsQueue[i].message;
    preferences.putString(key, data);
  } else if (preferences.isKey(key)) {
    preferences.remove(key);
  }
  preferences.end();
}

static bool parseQueueEntry(const String &d, SMSMessage &m) {
  int s1 = d.indexOf('|');
  int s2 = d.indexOf('|', s1 + 1);
  int s3 = d.indexOf('|', s2 + 1);
  if (s1 <= 0 || s2 < 0 || s3 < 0) return false;
  m.seq = strtoul(d.substring(0, s1).c_str(), nullptr, 10);
  m.retries = (uint8_t)d.substring(s1 + 1, s2).toInt();
  m.phone = d.substring(s2 + 1, s3);
  m.message = d.substring(s3 + 1);
  m.pending = true;
  m.lastAttempt = 0;
  return m.phone.length() > 0 && m.message.length() > 0;
}

// Format z wersji <= 2.6.x (klucz "sms_<i>"): phone|message|retries.
static bool parseLegacyQueueEntry(const String &d, SMSMessage &m) {
  int s1 = d.indexOf('|');
  int s2 = d.lastIndexOf('|');   // treść może zawierać '|'
  if (s1 <= 0 || s2 <= s1) return false;
  bool truncated;
  if (!normalizePhone(d.substring(0, s1), m.phone)) return false;
  m.message = sanitizeSmsText(d.substring(s1 + 1, s2), truncated);
  m.retries = (uint8_t)d.substring(s2 + 1).toInt();
  m.pending = true;
  m.lastAttempt = 0;
  return m.message.length() > 0;
}

void loadSMSQueue() {
  bool migrated[SMS_QUEUE_SIZE] = { false };
  uint32_t maxSeq = 0;

  preferences.begin(NVS_NAMESPACE, false);
  for (int i = 0; i < SMS_QUEUE_SIZE; i++) {
    char key[12], legacyKey[12];
    snprintf(key, sizeof(key), "q%d", i);
    snprintf(legacyKey, sizeof(legacyKey), "sms_%d", i);
    smsQueue[i] = SMSMessage();

    if (preferences.isKey(key)) {
      if (!parseQueueEntry(preferences.getString(key, ""), smsQueue[i])) {
        smsQueue[i] = SMSMessage();
        smsDroppedFromNVS++;
        preferences.remove(key);
      }
    }
    if (preferences.isKey(legacyKey)) {
      if (!smsQueue[i].pending) {
        SMSMessage m;
        if (parseLegacyQueueEntry(preferences.getString(legacyKey, ""), m)) { smsQueue[i] = m; migrated[i] = true; }
        else smsDroppedFromNVS++;
      }
      preferences.remove(legacyKey);
    }
    if (smsQueue[i].pending && !migrated[i] && smsQueue[i].seq > maxSeq) maxSeq = smsQueue[i].seq;
  }
  preferences.end();

  nextSeq = maxSeq + 1;
  for (int i = 0; i < SMS_QUEUE_SIZE; i++) {
    if (!migrated[i]) continue;
    smsQueue[i].seq = nextSeq++;
    saveQueueSlot(i);
  }
  recountPending();
  LOG("Queue loaded from NVS: pending=" + String(pendingCount) + " dropped=" + String(smsDroppedFromNVS));
}

void finishQueueSlot(int idx) {
  smsQueue[idx] = SMSMessage();
  saveQueueSlot(idx);
  recountPending();
}

// ---------------- Dedup ----------------
bool isSMSDuplicate(const String &phone, const String &message) {
  unsigned long now = millis();
  for (int i = 0; i < SMS_CACHE_SIZE; i++) {
    if (smsCache[i].active && smsCache[i].phone == phone && smsCache[i].message == message &&
        !timePassed(now, smsCache[i].timestamp, SMS_DEDUP_TIME)) {
      smsDuplicatesBlocked++;
      return true;
    }
  }
  return false;
}
void addToSMSCache(const String &phone, const String &message) {
  smsCache[smsCacheIndex] = { phone, message, millis(), true };
  smsCacheIndex = (smsCacheIndex + 1) % SMS_CACHE_SIZE;
}
bool isNumberAllowed(const String &number) {
  String n;
  if (!normalizePhone(number, n)) return false;
  if (n[0] == '+') n.remove(0, 1);
  for (size_t i = 0; i < ALLOWED_NUMBERS_COUNT; i++) {
    const char *a = ALLOWED_NUMBERS[i];
    if (*a == '+') a++;
    if (n == a) return true;
  }
  return false;
}
bool isIncomingSMSDuplicate(const String &sender, const String &message) {
  unsigned long now = millis();
  for (int i = 0; i < INCOMING_SMS_CACHE_SIZE; i++) {
    if (incomingSMSCache[i].active && incomingSMSCache[i].sender == sender && incomingSMSCache[i].message == message &&
        !timePassed(now, incomingSMSCache[i].timestamp, INCOMING_SMS_DEDUP_TIME)) {
      incomingDuplicatesBlocked++;
      return true;
    }
  }
  return false;
}
void addToIncomingSMSCache(const String &sender, const String &message) {
  incomingSMSCache[incomingSMSCacheIndex] = { sender, message, millis(), true };
  incomingSMSCacheIndex = (incomingSMSCacheIndex + 1) % INCOMING_SMS_CACHE_SIZE;
}
void cleanupSMSCache() {
  unsigned long now = millis();
  for (int i = 0; i < SMS_CACHE_SIZE; i++)
    if (smsCache[i].active && timePassed(now, smsCache[i].timestamp, SMS_DEDUP_TIME)) { smsCache[i].active = false; smsCache[i].phone = ""; smsCache[i].message = ""; }
}
void cleanupIncomingSMSCache() {
  unsigned long now = millis();
  for (int i = 0; i < INCOMING_SMS_CACHE_SIZE; i++)
    if (incomingSMSCache[i].active && timePassed(now, incomingSMSCache[i].timestamp, INCOMING_SMS_DEDUP_TIME)) { incomingSMSCache[i].active = false; incomingSMSCache[i].sender = ""; incomingSMSCache[i].message = ""; }
}

bool addSMSToQueue(const String &rawPhone, const String &rawMessage) {
  String phone;
  if (!normalizePhone(rawPhone, phone)) {
    LOG("✗ Invalid phone rejected: " + rawPhone);
    publishDiag("queue", "invalid_phone");
    return false;
  }
  bool truncated;
  String message = sanitizeSmsText(rawMessage, truncated);
  if (message.length() == 0) { LOG("✗ Empty message rejected"); return false; }
  if (truncated) LOG("⚠ Message truncated to " + String(SMS_MAX_LEN) + " chars");

  if (isSMSDuplicate(phone, message)) { LOG("⚠ Duplicate SMS blocked for " + phone); return false; }
  int freeIdx = findFreeSlot();
  if (freeIdx < 0) { LOG("✗ Queue full - SMS dropped"); publishDiag("queue", "full"); return false; }

  smsQueue[freeIdx] = { phone, message, true, 0, nextSeq++, 0UL };
  addToSMSCache(phone, message);
  saveQueueSlot(freeIdx);
  recountPending();

  if (pendingCount == 1) lastTxProgressMs = millis();   // start liczenia "TX stuck" dla pustej kolejki
  LOG("✓ Added 1 SMS to queue (pending=" + String(pendingCount) + ")");
  return true;
}

// ---------------- WiFi ----------------
void setupWiFi() {
  LOG("WiFi connecting to: " + String(WIFI_SSID));
  WiFi.persistent(false);
  WiFi.setAutoReconnect(true);
  WiFi.mode(WIFI_STA);

  if (USE_STATIC_IP) {
    IPAddress ip, gateway, subnet, dns1, dns2;
    ip.fromString(STATIC_IP);
    gateway.fromString(GATEWAY);
    subnet.fromString(SUBNET);
    dns1.fromString(DNS_PRIMARY);
    dns2.fromString(DNS_SECONDARY);
    WiFi.config(ip, gateway, subnet, dns1, dns2);
    LOG("Static IP: " + String(STATIC_IP));
  }

  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && !timePassed(millis(), start, WIFI_CONNECT_TIMEOUT_MS)) {
    delay(250);
    feedWatchdog();
    Serial.print(".");
  }
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    LOG("✓ WiFi connected!");
    LOG("IP: " + WiFi.localIP().toString());
    LOG("RSSI: " + String(WiFi.RSSI()) + " dBm");
  } else {
    // Bez restartu: modem i kolejka działają dalej, WiFi wznawia maintainWiFi().
    LOG("✗ WiFi not connected yet - will retry in background");
    wifiDown = true;
    wifiDownSince = millis();
    lastWifiReconnect = millis();
  }
}

// Nieblokujące utrzymanie WiFi (zamiast blokującego setupWiFi() w loop()).
void maintainWiFi(unsigned long now) {
  if (WiFi.status() == WL_CONNECTED) {
    if (wifiDown) {
      wifiDown = false;
      LOG("✓ WiFi reconnected, IP: " + WiFi.localIP().toString());
    }
    return;
  }
  if (!wifiDown) {
    wifiDown = true;
    wifiDownSince = now;
    lastWifiReconnect = now;
    LOG("⚠ WiFi connection lost");
    return;
  }
  if (timePassed(now, lastWifiReconnect, WIFI_RECONNECT_INTERVAL)) {
    LOG("WiFi reconnect...");
    WiFi.reconnect();
    lastWifiReconnect = now;
  }
  if (timePassed(now, wifiDownSince, WIFI_RESTART_AFTER_MS)) {
    LOG("✗ WiFi down too long - restarting ESP32");
    rememberAction("wifi_restart");
    delay(200);
    ESP.restart();
  }
}

// ---------------- MQTT ----------------
void publishDiscovery();
void publishStatus();

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  if (strcmp(topic, MQTT_TOPIC_SEND) != 0) return;
  if (length > MQTT_MAX_INCOMING_PAYLOAD) { LOG("✗ MQTT message too large"); return; }

  JsonDoc<1024> doc;
  // const => ArduinoJson kopiuje napisy (bufor PubSubClient może zostać nadpisany przy publish)
  if (deserializeJson(doc, (const byte*)payload, length)) { LOG("✗ JSON parse error"); return; }

  const char *message = doc["message"] | "";
  if (*message == '\0') return;

  if (!doc["phone"].isNull()) {
    addSMSToQueue(doc["phone"].as<String>(), message);
  } else if (doc["phones"].is<JsonArray>()) {
    for (JsonVariant v : doc["phones"].as<JsonArray>()) {
      if (!v.isNull()) addSMSToQueue(v.as<String>(), message);
    }
  } else {
    return;
  }
  statusDirty = true;   // status publikowany z loop(), nie z wnętrza callbacka
}

void setupMQTT() {
  mqttClient.setBufferSize(MQTT_BUFFER_SIZE);
  mqttClient.setKeepAlive(MQTT_KEEPALIVE_SEC);
  mqttClient.setSocketTimeout(MQTT_SOCKET_TIMEOUT_SEC);
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);
}

void mqttReconnect() {
  if (WiFi.status() != WL_CONNECTED) return;
  if (mqttClient.connected()) return;

  LOG("MQTT connecting to " + String(MQTT_BROKER) + "...");
  const char* lwt = "{\"state\":\"offline\"}";
  bool ok = mqttClient.connect(DEVICE_ID, MQTT_USER, MQTT_PASSWORD, MQTT_TOPIC_STATUS, 1, true, lwt);

  if (ok) {
    LOG("✓ MQTT connected!");
    mqttClient.subscribe(MQTT_TOPIC_SEND, 1);
    LOG("✓ Subscribed to: " + String(MQTT_TOPIC_SEND));
    publishDiscovery();
    publishStatus();
    if (!bootDiagSent) {
      publishDiag("boot", String("version=") + VERSION + " reset=" + resetReasonToStr(bootResetReason) +
                  " resets=" + String(totalResets) + " last_action=" + (lastAction.length() ? lastAction : "none"));
      bootDiagSent = true;
    }
  } else {
    LOG("✗ MQTT failed rc=" + String(mqttClient.state()));
  }
}

void publishDiscovery() {
  JsonDoc<512> doc;
  doc["name"] = "ESP32 SMS Gateway";
  doc["unique_id"] = DEVICE_ID;
  doc["command_topic"] = MQTT_TOPIC_SEND;
  doc["state_topic"] = MQTT_TOPIC_STATUS;

  JsonObject device = doc["device"].to<JsonObject>();
  device["identifiers"][0] = DEVICE_ID;
  device["name"] = "ESP32 SMS Gateway";
  device["model"] = "ESP32 + A7670E";
  device["sw_version"] = VERSION;
  device["manufacturer"] = "DIY";

  char buffer[512];
  size_t n = serializeJson(doc, buffer, sizeof(buffer));
  mqttClient.publish(MQTT_TOPIC_DISCOVERY, (uint8_t*)buffer, n, true);
}

void publishStatus() {
  statusDirty = false;
  if (!mqttClient.connected()) return;

  JsonDoc<768> doc;
  doc["state"] = modemReady ? "online" : "offline";
  doc["version"] = VERSION;
  doc["signal_csq"] = signalStrength;
  doc["signal_dbm"] = signalQuality;
  doc["operator"] = currentOperator;
  doc["queue"] = pendingCount;
  doc["sent"] = smsSentSuccess;
  doc["failed"] = smsSentFailed;
  doc["uncertain"] = smsSentUncertain;
  doc["dropped_nvs"] = smsDroppedFromNVS;
  doc["received"] = smsReceived;
  doc["commands"] = smsCommandsProcessed;
  doc["out_dup"] = smsDuplicatesBlocked;
  doc["in_dup"] = incomingDuplicatesBlocked;
  doc["unauthorized"] = unauthorizedAttempts;
  doc["heap"] = ESP.getFreeHeap();
  doc["min_heap"] = ESP.getMinFreeHeap();
  doc["stack_hwm_bytes"] = getStackHighWaterMarkBytes();
  doc["uptime"] = millis() / 1000;
  doc["resets"] = totalResets;
  doc["ip"] = WiFi.localIP().toString();

  char buffer[768];
  size_t n = serializeJson(doc, buffer, sizeof(buffer));
  mqttClient.publish(MQTT_TOPIC_STATUS, (uint8_t*)buffer, n, true);
}

// ---------------- Incoming SMS ----------------
void handleIncomingSMS(const String &sender, const String &timestamp, const String &body) {
  smsReceived++;
  statusDirty = true;
  LOG("◀ SMS from " + sender);

  if (isIncomingSMSDuplicate(sender, body)) { LOG("⚠ Duplicate incoming SMS ignored"); return; }
  addToIncomingSMSCache(sender, body);

  if (!isNumberAllowed(sender)) {
    unauthorizedAttempts++;
    LOG("✗ Unauthorized sender: " + sender);
    publishDiag("sms_in", "unauthorized from " + sender + ": " + body);
    return;
  }

  smsCommandsProcessed++;
  if (!mqttClient.connected()) { LOG("⚠ MQTT offline - incoming SMS not forwarded"); return; }

  JsonDoc<512> doc;
  doc["sender"] = sender;
  doc["message"] = body;
  doc["timestamp"] = timestamp;
  doc["authorized"] = true;
  char buf[640];
  size_t n = serializeJson(doc, buf, sizeof(buf));
  mqttClient.publish(MQTT_TOPIC_RECEIVED, (uint8_t*)buf, n, false);
}

// ---------------- Modem line reader / URC ----------------
// Cały ruch z modemu przechodzi przez jeden parser linii, więc niezamówione komunikaty (URC),
// np. +CMT z przychodzącym SMS-em, są obsługiwane zawsze - także w trakcie komend AT.
static char modemLineBuf[MODEM_LINE_MAX];
static size_t modemLineLen = 0;
static bool cmtAwaitingBody = false;
static String cmtSender, cmtTimestamp;

// Zwraca true, gdy w modemLineBuf jest kompletna linia.
static bool modemFeed(char c) {
  if (c == '\r') return false;
  if (c == '\n') {
    // pusta linia jest istotna tylko jako treść pustego SMS-a po nagłówku +CMT
    if (modemLineLen == 0 && !cmtAwaitingBody) return false;
    modemLineBuf[modemLineLen] = '\0';
    modemLineLen = 0;
    return true;
  }
  if (modemLineLen == 0 && c == ' ' && !cmtAwaitingBody) return false;
  if (modemLineLen < MODEM_LINE_MAX - 1) modemLineBuf[modemLineLen++] = c;
  return false;
}

// n-te pole w cudzysłowach, np. +CMT: "+48...","","24/10/05,12:00:00+08"
static String quotedField(const char *line, int n) {
  const char *p = line;
  for (int i = 0;; i++) {
    const char *a = strchr(p, '"');
    if (!a) return "";
    const char *b = strchr(a + 1, '"');
    if (!b) return "";
    if (i == n) return String(a + 1).substring(0, b - a - 1);
    p = b + 1;
  }
}

static bool isErrorLine(const char *line) {
  return strcmp(line, "ERROR") == 0 || strncmp(line, "+CME ERROR", 10) == 0 || strncmp(line, "+CMS ERROR", 10) == 0;
}

// Obsługuje URC. Zwraca true, jeśli linia została "skonsumowana".
static bool handleUrc(const char *line) {
  if (cmtAwaitingBody) {
    cmtAwaitingBody = false;
    handleIncomingSMS(cmtSender, cmtTimestamp, String(line));
    return true;
  }
  if (strncmp(line, "+CMT:", 5) == 0) {
    cmtSender = quotedField(line, 0);
    cmtTimestamp = quotedField(line, 2);
    cmtAwaitingBody = true;
    return true;
  }
  if (strncmp(line, "+CMTI:", 6) == 0) {
    LOG(String("SMS stored in memory: ") + line);
    publishDiag("sms_in", String("stored SMS (not forwarded): ") + line);
    return true;
  }
  if (strcmp(line, "RDY") == 0 || strncmp(line, "*ATREADY", 8) == 0) {
    if (!modemInitInProgress && modemReady) {
      LOG("⚠ Modem restarted unexpectedly -> reinit");
      modemReady = false;
      modemNeedsInit = true;
      statusDirty = true;
    }
    return true;
  }
  if (strcmp(line, "SMS DONE") == 0 || strcmp(line, "PB DONE") == 0) return true;
  return false;
}

// Odczytuje wszystko, co modem wysłał "sam z siebie" (wywoływane w loop() i przed każdą komendą).
void modemPoll() {
  while (SerialAT.available()) {
    if (!modemFeed((char)SerialAT.read())) continue;
    if (!handleUrc(modemLineBuf)) LOG(String("modem: ") + modemLineBuf);
  }
}

// Wysyła komendę i czeka na OK / ERROR. Linie odpowiedzi (bez URC) trafiają do resp.
AtResult atCommand(const char *cmd, unsigned long timeout = AT_COMMAND_TIMEOUT, String *resp = nullptr) {
  modemPoll();   // opróżnij bufor RX - stare "OK" nie zostanie wzięte za odpowiedź
  if (resp) *resp = "";
  SerialAT.print(cmd);
  SerialAT.print('\r');

  unsigned long start = millis();
  while (!timePassed(millis(), start, timeout)) {
    while (SerialAT.available()) {
      if (!modemFeed((char)SerialAT.read())) continue;
      const char *line = modemLineBuf;
      if (handleUrc(line)) continue;
      if (strcmp(line, "OK") == 0) return AT_OK;
      bool err = isErrorLine(line);
      if (resp && strcmp(line, cmd) != 0) { *resp += line; *resp += '\n'; }
      if (err) return AT_ERROR;
    }
    serviceBackground();
    delay(2);
  }
  return AT_TIMEOUT;
}

// ---- cold boot modem helpers ----
static void modemPwrkeyIdleHigh() {
  pinMode(MODEM_PWRKEY, OUTPUT);
  digitalWrite(MODEM_PWRKEY, HIGH);
  delay(50);
}

static void modemPwrkeyPulseOn() {
  // typowo A7670: PWRKEY aktywne LOW (przycisk do GND)
  digitalWrite(MODEM_PWRKEY, HIGH);
  delay(100);
  digitalWrite(MODEM_PWRKEY, LOW);
  waitMs(MODEM_PWRKEY_PULSE_MS);
  digitalWrite(MODEM_PWRKEY, HIGH);
}

static bool modemATProbe(unsigned long probeMs) {
  unsigned long start = millis();
  while (!timePassed(millis(), start, probeMs)) {
    if (atCommand("AT", 800) == AT_OK) return true;
    waitMs(200);
  }
  return false;
}

static void modemHardReset() {
  if (MODEM_RESET_N < 0) return;
  LOG("Modem hard reset (RESET_N)");
  pinMode(MODEM_RESET_N, OUTPUT);
  digitalWrite(MODEM_RESET_N, HIGH);
  delay(50);
  digitalWrite(MODEM_RESET_N, LOW);
  waitMs(2600); // SIMCom zaleca ~2.5s jako reset pulse
  digitalWrite(MODEM_RESET_N, HIGH);
  waitMs(200);
}

static bool modemPowerOnAndProbe() {
  modemPwrkeyIdleHigh();

  // 1) Najpierw sprawdź, czy już żyje (czasem modem sam startuje po VBAT)
  if (modemATProbe(MODEM_BOOT_PROBE_MS)) return true;

  // 2) Cisza: impuls PWRKEY i długie okno na start (zbyt szybki kolejny impuls wyłączyłby modem)
  for (int i = 0; i < MODEM_POWERON_TRIES; i++) {
    if (MODEM_STATUS_PIN >= 0 && digitalRead(MODEM_STATUS_PIN) == HIGH) {
      LOG("Modem STATUS=ON - skipping PWRKEY pulse");
    } else {
      LOG("Modem power-on attempt " + String(i + 1) + "/" + String(MODEM_POWERON_TRIES));
      modemPwrkeyPulseOn();
      waitMs(MODEM_AFTER_PULSE_WAIT_MS);
    }
    if (modemATProbe(MODEM_AFTER_PULSE_PROBE_MS)) return true;
  }

  // 3) Ostatecznie: twardy reset (jeśli pin jest podpięty)
  LOG("✗ Modem not responding");
  publishDiag("modem", "no AT response after coldboot tries");
  if (MODEM_RESET_N >= 0) {
    modemHardReset();
    return modemATProbe(MODEM_AFTER_PULSE_PROBE_MS);
  }
  return false;
}

// ---------------- Modem ----------------
void setupAndOptionallyCleanSmsStorage() {
  atCommand("AT+CPMS=\"SM\",\"SM\",\"SM\"", 3000);
  if (SMS_STORAGE_CLEAN_ON_BOOT) atCommand("AT+CMGD=1,4", 8000);
  String r;
  if (atCommand("AT+CPMS?", 2000, &r) == AT_OK && r.length() > 0) LOG("CPMS: " + r);
}

void checkModemFirmware() {
  String r;
  if (atCommand("AT+CGMM", 1500, &r) == AT_OK) LOG("Model: " + r);
  if (atCommand("AT+GSN", 1500, &r) == AT_OK) LOG("IMEI: " + r);
}

static inline bool signalOk() { return signalStrength != 99 && signalStrength >= SIGNAL_THRESHOLD; }

bool checkSignalQuality() {
  lastSignalCheck = millis();
  String r;
  if (atCommand("AT+CSQ", 2000, &r) != AT_OK) return false;
  int p = r.indexOf("+CSQ:");
  if (p < 0) return false;
  int csq = r.substring(p + 5).toInt();   // toInt() kończy na ','
  if (csq != signalStrength) statusDirty = true;
  signalStrength = csq;
  signalQuality = (csq == 99) ? -999 : -113 + (csq * 2);
  return signalOk();
}

void updateOperator() {
  String r;
  if (atCommand("AT+COPS?", 5000, &r) != AT_OK) return;
  String op = quotedField(r.c_str(), 0);
  if (op != currentOperator) { currentOperator = op; statusDirty = true; }
  if (op.length() > 0) LOG("Operator: " + op);
}

static bool waitForSimReady() {
  String r;
  for (int i = 0; i < MODEM_SIM_READY_TRIES; i++) {
    if (atCommand("AT+CPIN?", 5000, &r) == AT_OK && r.indexOf("READY") != -1) return true;
    waitMs(2000);
  }
  LOG("✗ SIM not ready: " + r);
  publishDiag("modem", "sim_not_ready " + r);
  return false;
}

static bool modemConfigure() {
  atCommand("ATE0", 2000);
  atCommand("AT+CMEE=2", 2000);

  checkModemFirmware();

  atCommand("AT+CSCLK=0", 2000);
  if (!waitForSimReady()) return false;

  String r;
  if (atCommand("AT+CREG?", 3000, &r) == AT_OK) LOG("CREG: " + r);

  checkSignalQuality();
  updateOperator();

  if (atCommand("AT+CMGF=1", 2000) != AT_OK) { LOG("✗ AT+CMGF=1 failed"); return false; }
  atCommand("AT+CSCS=\"GSM\"", 2000);
  atCommand("AT+CTZU=1", 2000);
  atCommand("AT+CSMP=17,167,0,0", 2000);

  setupAndOptionallyCleanSmsStorage();
  if (atCommand("AT+CNMI=2,2,0,0,0", 2000) != AT_OK) { LOG("✗ AT+CNMI failed"); return false; }
  return true;
}

bool initModem() {
  LOG("Init A7670E modem...");
  modemInitInProgress = true;
  modemReady = false;
  modemNeedsInit = false;
  lastModemInitAttempt = millis();

  bool ok = modemPowerOnAndProbe() && modemConfigure();

  modemInitInProgress = false;
  modemReady = ok;
  statusDirty = true;
  lastModemInitAttempt = millis();
  if (ok) {
    LOG("✓ Modem ready!");
  } else {
    LOG("✗ Modem init failed - retry in " + String(MODEM_REINIT_INTERVAL / 1000) + "s");
    publishDiag("modem", "init_failed");
  }
  return ok;
}

void resetModem() {
  LOG("═══ MODEM RESET ═══");
  modemReady = false;
  statusDirty = true;
  if (MODEM_RESET_N >= 0) {
    modemHardReset();
  } else if (atCommand("AT+CRESET", 3000) == AT_OK) {
    LOG("AT+CRESET sent, waiting for modem reboot...");
    waitMs(MODEM_CRESET_WAIT_MS);
  }
  initModem();
}

bool checkModemHealth() { return modemATProbe(3000); }

// ---------------- SMS send ----------------
SendResult sendSMS(const String &number, const String &message) {
  LOG("▶ Sending SMS to " + number);

  modemPoll();
  SerialAT.print("AT+CMGS=\"");
  SerialAT.print(number);
  SerialAT.print("\"\r");

  unsigned long start = millis();
  bool promptReceived = false;
  while (!promptReceived && !timePassed(millis(), start, SMS_PROMPT_TIMEOUT)) {
    while (SerialAT.available()) {
      char c = (char)SerialAT.read();
      if (c == '>' && modemLineLen == 0 && !cmtAwaitingBody) { promptReceived = true; break; }
      if (!modemFeed(c)) continue;
      if (handleUrc(modemLineBuf)) continue;
      if (isErrorLine(modemLineBuf)) {
        LOG(String("✗ AT+CMGS rejected: ") + modemLineBuf);
        publishDiag("sms", String("cmgs_error ") + modemLineBuf);
        return SEND_FAIL;
      }
    }
    if (!promptReceived) { serviceBackground(); delay(2); }
  }
  if (!promptReceived) {
    SerialAT.write(27);   // ESC - anuluj rozpoczęte AT+CMGS
    LOG("✗ No '>' prompt");
    publishDiag("sms", "no_prompt");
    return SEND_FAIL;
  }

  SerialAT.print(message);
  SerialAT.write(26);

  start = millis();
  bool gotCMGS = false;
  while (!timePassed(millis(), start, SMS_SEND_TIMEOUT)) {
    while (SerialAT.available()) {
      if (!modemFeed((char)SerialAT.read())) continue;
      const char *line = modemLineBuf;
      if (handleUrc(line)) continue;
      if (strncmp(line, "+CMGS:", 6) == 0) { gotCMGS = true; continue; }
      if (strcmp(line, "OK") == 0) {
        if (gotCMGS) {
          LOG("✓ SMS sent successfully (+CMGS + OK)");
          waitMs(SMS_COOLDOWN_MS);
          return SEND_OK;
        }
        LOG("⚠ OK without +CMGS");
        publishDiag("sms", "ok_without_cmgs");
        return SEND_UNCERTAIN;
      }
      if (isErrorLine(line)) {
        LOG(String("✗ SMS error: ") + line);
        publishDiag("sms", String("send_error ") + line);
        return SEND_FAIL;
      }
    }
    serviceBackground();
    delay(5);
  }

  publishDiag("sms", gotCMGS ? "timeout after +CMGS (no OK)" : "uncertain timeout");
  return SEND_UNCERTAIN;
}

// ---------------- TX watchdog / send ----------------
void txWatchdogAndDiag(unsigned long now) {
  if (!modemReady || pendingCount == 0) return;

  if (timePassed(now, lastTxBlockLogMs, TX_BLOCK_LOG_MS)) {
    unsigned long ageSec = (lastTxProgressMs == 0) ? 0 : (elapsedMs(now, lastTxProgressMs) / 1000);
    LOG("TX diag: pending=" + String(pendingCount) + " head=" + String(findNextPending()) +
        " CSQ=" + String(signalStrength) + " lastProgress=" + String(ageSec) + "s");
    lastTxBlockLogMs = now;
  }

  if (lastTxProgressMs != 0 && timePassed(now, lastTxProgressMs, TX_STUCK_RESET_MS)) {
    LOG("⚠ TX stuck -> resetting modem");
    publishDiag("modem", "tx_stuck_reset");
    resetModem();
    lastTxProgressMs = millis();
  }
}

void processSMSQueue() {
  unsigned long now = millis();
  lastSMSSent = now;   // throttling także wtedy, gdy wysyłka jest zablokowana

  if (!signalOk()) {
    if (timePassed(now, lastSignalCheck, SIGNAL_RECHECK_BLOCKED_MS)) checkSignalQuality();
    if (!signalOk()) {
      if (!txBlockedLogged) { LOG("⚠ TX blocked: CSQ=" + String(signalStrength)); txBlockedLogged = true; }
      return;
    }
  }
  txBlockedLogged = false;

  int idx = findNextSendable(now);
  if (idx < 0) return;

  SendResult res = sendSMS(smsQueue[idx].phone, smsQueue[idx].message);
  now = millis();

  switch (res) {
    case SEND_OK:
      smsSentSuccess++;
      finishQueueSlot(idx);
      lastTxProgressMs = now;
      break;
    case SEND_UNCERTAIN:
      // SMS mógł zostać wysłany - bez ponawiania, żeby nie zdublować wiadomości
      smsSentUncertain++;
      LOG("⚠ SMS status uncertain - not retrying to avoid duplicate");
      finishQueueSlot(idx);
      lastTxProgressMs = now;
      break;
    case SEND_FAIL:
      smsQueue[idx].retries++;
      smsQueue[idx].lastAttempt = now;
      if (smsQueue[idx].retries > SMS_MAX_RETRIES) {
        smsSentFailed++;
        LOG("✗ SMS failed after " + String(smsQueue[idx].retries) + " attempts - dropped");
        finishQueueSlot(idx);
        lastTxProgressMs = now;
      } else {
        saveQueueSlot(idx);   // licznik prób przetrwa restart
      }
      break;
  }

  lastSMSSent = now;
  statusDirty = true;
}

// ---------------- health ----------------
void checkHeapMemory() {
  uint32_t heap = ESP.getFreeHeap();
  if (heap < 50000) LOG("⚠ LOW HEAP: " + String(heap));
}

// ================== SETUP/LOOP ==================
void setup() {
  bool cpuOk = setCpuFrequencyMhz(CPU_FREQ_MHZ);
  Serial.begin(115200);
  delay(500);
  LOG("ESP32 SMS Gateway " VERSION);
  if (!cpuOk) LOG("⚠ setCpuFrequencyMhz(" + String(CPU_FREQ_MHZ) + ") failed");
  LOG("CPU: " + String(getCpuFrequencyMhz()) + " MHz");

  printResetReason();

  esp_task_wdt_deinit();
  esp_task_wdt_config_t wdt_config = {.timeout_ms=WDT_TIMEOUT,.idle_core_mask=(1<<0),.trigger_panic=true};
  esp_err_t wdt_result = esp_task_wdt_init(&wdt_config);
  if (wdt_result == ESP_OK || wdt_result == ESP_ERR_INVALID_STATE) {
    esp_task_wdt_add(NULL);
    watchdogEnabled = true;
    LOG("✓ Watchdog enabled");
  }

  loadSMSQueue();

  if (MODEM_STATUS_PIN >= 0) pinMode(MODEM_STATUS_PIN, INPUT);
  SerialAT.setRxBufferSize(4096);
  SerialAT.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);

  setupWiFi();
  setupMQTT();
  mqttReconnect();

  initModem();

  unsigned long now = millis();
  lastModemCheck = now;
  lastHeapCheck = now;
  lastSMSSent = now;
  lastTxProgressMs = now;
  lastTxBlockLogMs = now;
  lastMQTTReconnect = now;

  LOG("SYSTEM READY");
}

void loop() {
  unsigned long now = millis();
  feedWatchdog();

  maintainWiFi(now);

  if (mqttClient.connected()) {
    mqttClient.loop();
  } else if (WiFi.status() == WL_CONNECTED && timePassed(now, lastMQTTReconnect, MQTT_RECONNECT_INTERVAL)) {
    mqttReconnect();
    lastMQTTReconnect = millis();
  }

  modemPoll();   // przychodzące SMS-y (+CMT) i inne URC

  if (modemNeedsInit || (!modemReady && timePassed(now, lastModemInitAttempt, MODEM_REINIT_INTERVAL))) {
    initModem();
    now = millis();
  }

  if (modemReady && timePassed(now, lastModemCheck, MODEM_CHECK_INTERVAL)) {
    if (!checkModemHealth()) resetModem();
    lastModemCheck = millis();
  }

  if (modemReady && timePassed(now, lastSignalCheck, SIGNAL_CHECK_INTERVAL)) {
    checkSignalQuality();
  }

  if (modemReady && pendingCount > 0 && timePassed(now, lastSMSSent, SMS_SEND_DELAY)) {
    processSMSQueue();
  }

  txWatchdogAndDiag(millis());

  if (timePassed(now, lastHeapCheck, HEAP_CHECK_INTERVAL)) {
    checkHeapMemory();
    cleanupSMSCache();
    cleanupIncomingSMSCache();
    lastHeapCheck = now;
  }

  if (statusDirty && mqttClient.connected()) publishStatus();

  delay(5);
}
