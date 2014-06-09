function values=daily(raw, firstM, lastM, measurementOffset)
  values = [];

  for m=firstM:lastM
    valuesM = [];
    for s=0:199
      curr = regr(raw, s, m, 3, measurementOffset, 1);
      valuesM = [valuesM curr(m)];
    end

    values = [values valuesM'];
  end
