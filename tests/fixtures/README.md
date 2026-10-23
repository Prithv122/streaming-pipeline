# Window fixtures

`orders_late_small.jsonl` is a 183-event log (3 zones, 5 windows plus one event in a sixth).
`windows_expected.json` holds the records, drops and peak state that `WindowCore` must produce
from it, for L = 0 (both modes) and L = 30 (wait and revise). The expected values were worked
out by hand from the rules in `GUIDELINES.md` before being compared with the code. Records are
`[emitted_at, zone, window_start, revision, final, orders, revenue_cents, late_dropped]` in emit
order; the end-of-stream flush happens at stream time 310.0.

## How the log is built

`build_small_log()` in `tests/test_windows.py` writes it, and a test checks the committed file
equals its output.

**Regular events.** Window k = `[60k, 60k+60)`, k = 0..4. For zone offset j (north 0, south 1,
east 2) and i = 0..11: `event_time = 60k + 5i + j`, `arrival = event_time + 0.5`. Amounts are
fixed per zone: north 10.00 (1000 c), south 20.50 (2050 c), east 5.25 (525 c). East has no
regular events in window 1. So every regular (zone, window) holds 12 orders, worth 12 000 c
(north), 24 600 c (south) or 6 300 c (east).

Regular events arrive in event-time order, so `M` is the latest regular event time, and a
window ending at `e` becomes due at the arrival of the regular north event with
`event_time = e` (arrival `e + 0.5`) and closed under L = 30 at the one with `event_time = e + 30`
(arrival `e + 30.5`). Windows that close in the same step fire in (window_end, zone) order:
east, north, south.

**Special events.** Arrivals end in .25, .75 or 0.0, so they never tie with a regular arrival.
`M` is the value before the event.

| id (ord-) | zone | event_time (window) | cents | arrives | M then | L = 0 | L = 30 |
|---|---|---|---|---|---|---|---|
| 900001 | north | 59.999 (0) | 100 | 60.0 | 57 | accepted | accepted (last event below 60) |
| 900002 | north | 50.0 (0) | 300 | 70.25 | 66 | dropped (66 >= 60) | accepted (66 < 90), window due |
| 900003 | south | 59.5 (0) | 125 | 75.25 | 71 | dropped | accepted, window due |
| 900004 | east | 10.0 (0) | 777 | 95.25 | 91 | dropped | dropped (91 >= 90) |
| 900006 | south | 120.0 (2) | 600 | 120.25 | 116 | accepted | accepted; M = 120 makes window 1 due |
| 900005 | east | 120.0 (2) | 200 | 120.75 | 120 | accepted | accepted (exactly on the boundary) |
| 900007 | north | 118.0 (1) | 250 | 125.25 | 122 | dropped (122 >= 120) | accepted (122 < 150), window due |
| 900008 | east | 100.0 (1) | 400 | 130.25 | 127 | dropped | accepted; **creates** east window 1, which had no event, already due |
| 900009 | north | 119.999 (1) | 999 | 155.25 | 152 | dropped | dropped (152 >= 150) |
| 900010 | south | 170.0 (2) | 500 | 190.25 | 187 | dropped | accepted (187 < 210) |
| 900011 | east | 175.0 (2) | 800 | 200.25 | 197 | dropped | accepted |
| 900012 | north | 130.0 (2) | 1200 | 215.25 | 212 | dropped | dropped (212 >= 210) |
| 900013 | north | 200.0 (3) | 1100 | 245.25 | 242 | dropped | accepted (242 < 270) |
| 900014 | north | 305.0 (5) | 100 | 305.25 | 297 | accepted, opens window 5; makes window 4 due | same, window 4 due |
| 900015 | south | 290.0 (4) | 200 | 306.25 | 305 | dropped (305 >= 300) | accepted (305 < 330) |

Late events: 11 arrive after their window is due (all but 900001, 900005, 900006, 900014), of
which 8 are within L = 30 and 3 beyond it. East window 1 is the late-created window.

## Deriving the L = 0 rows (wait and revise are the same)

A window closes the moment `M >= window_end`, so every row is final, revision 0.

- Window 0 closes at 60.5 (north event 60). Orders: east 12; north 12 + 900001 = 13, 12 100 c;
  south 12. Nothing dropped yet.
- Window 1 closes at 120.25 (event 900006, `M = 120`), before the regular 120.5. North 12,
  south 12; the zone counters are 1 each (900002 for north, 900003 for south). East has no
  window 1.
- Window 2 closes at 180.5. East 12 + 900005 = 13, 6 500 c; north 12 (900012 comes later);
  south 12 + 900006 = 13, 25 200 c. Counters at that moment: north 3 (900002, 900007, 900009),
  south 1, east 2 (900004, 900008).
- Window 3 closes at 240.5. 12 each; counters north 4 (+900012), south 2 (+900010), east 3 (+900011).
- Window 4 closes at 305.25, in the step where 900014 arrives. 12 each; counters north 5
  (+900013), south 2, east 3. Then 900015 is dropped.
- Flush at 310.0: north window 5 (900014), 1 order, 100 c, north counter 5.

Peak state is 4: at 60.5 the three window-0 states are still held when the new window 1 is
created (3 + 1), and the same happens at 180.5, 240.5 and 305.25.

## Deriving the L = 30 wait rows

Windows close at `e + 30`: 90.5, 150.5, 210.5, 270.5; windows 4 and 5 stay open until flush.
A record carries every accepted event, including late ones accepted silently.

- 90.5: east 12; north 12 + 900001 + 900002 = 14, 12 400 c; south 12 + 900003 = 13,
  24 600 + 125 = 24 725 c. All counters 0 (900004 arrives later).
- 150.5: east window 1 = 900008 only, 400 c, east counter 1; north 12 + 900007 = 13,
  12 000 + 250 = 12 250 c; south 12.
- 210.5: east 12 + 900005 + 900011 = 14, 6 300 + 200 + 800 = 7 300 c; north 12, counter 1
  (900009); south 12 + 900006 + 900010 = 14, 24 600 + 600 + 500 = 25 700 c.
- 270.5: east 12; north 12 + 900013 = 13, 13 100 c, counter 2 (900009, 900012); south 12.
- Flush at 310.0, (end, zone) order: east 4 (12, counter 1), north 4 (12, counter 2), south 4
  (12 + 900015 = 13, 24 800 c), then north 5 (1 order, counter 2).
- Dropped: 900004, 900009, 900012.

Peak state is 6, reached first at 130.25 when east window 1 is created: north 1, south 1,
south 2, north 2, east 2 are open (5) plus the new one. Windows 2, 3 and 4 give the same peak.

## Deriving the L = 30 revise rows

Same accepted events and same final values. Extra records:

- Provisional revision 0 when a window becomes due: 60.5 (window 0, east, north, south), 120.25
  (window 1, north and south), 180.5 (window 2), 240.5 (window 3), 305.25 (window 4).
- One revision per accepted event into a due window: 900002 (70.25), 900003 (75.25), 900007
  (125.25), 900008 (130.25, revision 0 of the new east window), 900010 (190.25), 900011 (200.25),
  900013 (245.25), 900015 (306.25).
- Finals at 90.5, 150.5, 210.5, 270.5 and at flush. A final is last revision + 1: for example
  north window 0 has revisions 0 (60.5), 1 (70.25) and final 2 (90.5); south window 1 has
  revision 0 (120.25) and final 1 (150.5); north window 5 never emitted, so its flush record is
  revision 0.
- Total 38 records: 8 + 7 + 8 + 7 + 7 + 1 per window (window 5 only has the flush record) and
  per-window revision counts are in the JSON.

## Hand-derivation mistakes caught while writing the tests

- A first draft of the small "late beyond L" test expected a provisional record for a window
  that was not yet due (M = 95 < 120). The code was right.
- The first version of event 900006 had event time 125.0 and arrived at 117.25, before its own
  event time, which breaks the log contract (`arrival_time >= event_time`). The same applied to
  900001 (59.75 for 59.999). Both were corrected in the log and in the expected rows.
