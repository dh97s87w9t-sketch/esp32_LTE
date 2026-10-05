// Skopiuj ten plik jako secrets.h (obok esp32_sms_gateway.ino) i uzupełnij dane.
// secrets.h jest w .gitignore - nie trafi do repozytorium.
#pragma once

#define WIFI_SSID "twoja_siec"
#define WIFI_PASSWORD "haslo_wifi"

#define MQTT_USER "uzytkownik_mqtt"
#define MQTT_PASSWORD "haslo_mqtt"

// Opcjonalnie: numery uprawnione do wysyłania SMS-ów do bramki (bez '+').
// Jeśli zdefiniujesz, nadpisują listę domyślną z kodu.
// #define SMS_ALLOWED_NUMBERS "48600000000", "48700000000"
