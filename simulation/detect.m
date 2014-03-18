% write initial version based on threshold
% only support detection over a single day (during periods of non-negliglble irradiation)

% maintain: previously "good" window of sampels
% iterate throughout the day for a single module
% update "good" if delta<threshold, or indicate failure.

% NOTE: for now it will continually replace the good window and never terminate
% NOTE: for now only some samples of the day are considered (range of s)

% outputs a column vector for every module, indicating detected failures with '1'

function output=detect(raw, firstM, lastM, measurementOffset, threshold)
  output = [];

  for m=firstM:lastM
    valuesM = [];
    for s=0:240
      curr = regr(raw, s, m, 3, measurementOffset, 1);
      valuesM = [valuesM curr(m)];
    end

    % iterate over daily difference and perform thresholding
    % 1 indicates failure over the corresponding window
    outputM = [0];
    windowSize = 16;
    window = filter(ones(1, windowSize)/windowSize, 1, valuesM');

    for t=windowSize:length(valuesM)-windowSize
      if abs(window(t + windowSize) - window(t)) > threshold
        outputM = [outputM 1];
      else
        outputM = [outputM 0];
      end
    end

    output = [output outputM'];
  end

%TODO
% * verify that non-faulty days are not classified as faulty, might need tuning during shadowing
%   -> need to place every day in sequentially numbered files and write a matlab wrapper
% * investigate the capability of detecting minor faults <- interval? [0.5 0.9]
%   -> inject faults during 2014 between 09 and 16
% * conclude whetever this approach is "good enough": yep!
% * extend to night->day transitions
