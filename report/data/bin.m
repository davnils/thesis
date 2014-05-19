function out=bin(m)
  x = m(:,1);
  y = m(:,2);

  topEdge = 1; % define limits
  botEdge = 0.94; % define limits
  numBins = 15; % define number of bins

  binEdges = linspace(botEdge, topEdge, numBins+1);

  [h,whichBin] = histc(x, binEdges);

  for i = 1:numBins
      flagBinMembers = (whichBin == i);
      binMembers     = y(flagBinMembers);
      out(i)         = mean(binMembers);
  end
