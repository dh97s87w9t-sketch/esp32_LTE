# Changelog

## 2.7.0-prod

Przegląd kodu wersji 2.6.3 i poprawki. Kod źródłowy przeniesiony z pliku
`VERSION "2.6.3-prod-coldboot-modemfix` do `esp32_sms_gateway/esp32_sms_gateway.ino`.

### Bezpieczeństwo
- Numer telefonu z MQTT jest walidowany (tylko cyfry, opcjonalny `+`), co blokuje
  wstrzyknięcie komend AT przez `AT+CMGS="..."`.
- Z treści SMS usuwane są znaki sterujące (m.in. Ctrl-Z i ESC, które przerywały wysyłkę).
  Polskie znaki są zamieniane na ASCII, inne znaki spoza ASCII na `?`, a treść jest przycinana do 160 znaków.

### Nowe: odbiór SMS
- Przychodzące SMS-y (`+CMT`) są odbierane i publikowane na `homeassistant/sms/received`:
  `{"sender":"+48...","message":"...","timestamp":"yy/MM/dd,hh:mm:ss+zz","authorized":true}`.
- Publikowane są tylko SMS-y od numerów z `ALLOWED_NUMBERS`. Pozostałe są liczone
  (`unauthorized`) i raportowane w `homeassistant/sms/diag`.
- Wszystkie odpowiedzi modemu przechodzą przez jeden parser linii, więc niezamówione
  komunikaty modemu (URC) nie psują już odpowiedzi na komendy AT.

### Niezawodność
- Gdy modem nie wystartuje, inicjalizacja jest ponawiana co 60 s (wcześniej bramka
  przestawała działać aż do restartu).
- Nieoczekiwany restart modemu (`*ATREADY` / `RDY`) wywołuje ponowną konfigurację.
- Bez pinu RESET_N reset modemu wykonywany jest przez `AT+CRESET`.
- Opcjonalny `MODEM_STATUS_PIN`: przy włączonym modemie impuls PWRKEY jest pomijany.
  Dłuższe okno po impulsie zmniejsza ryzyko wyłączenia wolno startującego modemu.
- Bufor RX jest opróżniany przed każdą komendą AT, a komendy kończą się na `OK`/`ERROR`
  zamiast czekać do upływu limitu czasu.
- SMS o niepewnym statusie (brak `+CMGS`/`OK` w czasie) nie jest wysyłany ponownie,
  więc nie powstają duplikaty.
- Brak sygnału nie zalewa już logów (wcześniej ~200 wpisów/s), a CSQ jest wtedy odświeżane co 15 s.
- WiFi utrzymywane jest bez blokowania pętli. Restart ESP następuje dopiero po 10 min
  bez sieci i jest raportowany po starcie (`boot` w diag).
- MQTT jest obsługiwane także podczas długich operacji modemu (keepalive).
- Poprawiono `setCpuFrequencyMhz(100)`: ta wartość jest niedozwolona i ESP pracował na 240 MHz.
  Teraz ustawiane jest 80 MHz.

### Kolejka
- Kolejka działa w kolejności FIFO (numer sekwencyjny).
- Zapis w NVS dotyczy tylko zmienionego slotu. Nowy format `q<i>` = `seq|retries|phone|message`,
  więc treść może zawierać `|`. Stare wpisy `sms_<i>` są migrowane automatycznie.
- Licznik prób zapisuje się w NVS, więc wiadomości po nieudanej próbie nie giną po restarcie.
- `SMS_MAX_RETRIES` oznacza teraz faktyczną liczbę ponowień (łącznie 3 próby), z odstępem 10 s.

### Pamięć i wydajność
- Usunięto nieczytany bufor 100 logów (`String`), który zajmował do ~10 KB sterty.
- Napisy są przekazywane przez `const String&` zamiast kopii.
- Status jest publikowany z `loop()` (flaga), a nie z wnętrza callbacka MQTT.
- Kod kompiluje się bez ostrzeżeń z ArduinoJson 6 i 7 (arduino-esp32 3.3.x).
- Status MQTT: usunięto pole `log_cleanups`.
