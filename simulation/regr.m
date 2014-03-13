function diffs=regr(raw, skip, module, measurements, measurementOffset, window)
  % build a vector of all differences in a window
  diffs = [];
  for w=0:window-1;
    diffs = [diffs regrSingle(raw, skip+w, module, measurements, measurementOffset)];
  end

function diffs=regrSingle(raw, skip, module, measurements, measurementOffset)
  % assume raw on format [m1 m2 m3 m1' m2' m3' ...]
  % skip is the number of 5-min samples to be skipped
  [rows cols] = size(raw);
  modules = cols/measurements;
  ref = raw(1 + skip, 1 + (module-1) * measurements + measurementOffset);

  diffs = [];
  for i=0:modules-1
    new = ref - raw(1 + skip, 1 + i * measurements + measurementOffset);
    diffs = [diffs new];
  end
