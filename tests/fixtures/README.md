`time_profile_dedup.xml` is the original captured frame-reference regression fixture.

The `xcode27_*.xml` fixtures reduce the first rows and schema metadata exported
from native TodoInstruments recordings with Xcode 27.2. CPU stacks retain three
frames; device-session values were already redacted to `TODO`. Reference-only
repetitions and a zero timestamp exercise variants of the same XML encoding.
The updates fixture preserves the added downstream columns before cause-graph-node.

`cpu_profile_cycles.xml` is a contract variant of the reduced CPU fixture with
cpu-profile/cycle-weight metadata. It pins the distinct hardware-cycle unit;
it is not evidence of a real CPU Profiler recording run.
