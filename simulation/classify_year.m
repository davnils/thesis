function result=classifyYear()
  clear
  for i=1:365
    file = sprintf('output_%i', i);
    run(file)
    % voltage (0): 0.3
    % current (1): 0.6
    res = detect2(output_', 1, 24, 1, 0.6);

    if any(any(res)) == 1
      i
    end
  end

% find the first window (at the same time across all modules)
% identified by checking if power >= 50 W
% this will have 2*24 entries
function window=findFirstWindow()
  % TODO

function smoothed=getSmoothedDay(raw, firstM, lastM)
  smoothed=[];
  for m=firstM:lastM
    values = [];
    for s=0:240
      volt = regr(raw, s, m, 3, 0, 1);
      curr = regr(raw, s, m, 3, 1, 1);
      values = [values curr(m)];
    end

    %TODO
    windowSize = 16;
    window = filter(ones(1, windowSize)/windowSize, 1, values');
    smoothed = [];

function result=detect2(raw, firstM, lastM, measurementOffset, threshold, prevWindows)
  output = [];

  smoothed = getSmoothedDay();
  for m=firstM:lastM
    % iterate over daily difference and perform thresholding
    % 1 indicates failure over the corresponding window
    outputM = [0];
    window = smoothed(); 

    for t=windowSize:length(valuesM)-windowSize
      if abs(window(t + windowSize) - window(t)) > threshold
        outputM = [outputM 1];
      else
        outputM = [outputM 0];
      end
    end

    output = [output outputM'];
  end

  result = [output blargh];
