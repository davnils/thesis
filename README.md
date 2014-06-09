# Fault detection in photovoltaic systems

This is my (preliminary) master's thesis written on the topic of detecting faults in solar panels.

## Abstract

This master's thesis concerns three different areas in the field of fault detection in photovoltaic systems.
Previous studies have concerned homogeneous systems with a large set of parameters being observed, while this study is focused on a more restrictive case.
The first problem is to discover immediate faults occurring in solar panels.
A new online algorithm is developed based on similarity measures within a single installation.
It performs reliably and is able to detect all significant faults over a certain threshold.
The second problem concerns measuring degradation over time.
A modified approach is taken based on repetitive conditions, and performs well given certain assumptions.
Finally the third problem is to differentiate solar panel faults from partial shading.
Here a clustering algorithm DBSCAN is applied on data in order to locate clusters of faults in the solar plane, demonstrating good performance in certain situations.
It also demonstrates issues with missclassification of real faults due to clustering.