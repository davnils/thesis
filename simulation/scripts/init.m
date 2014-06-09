function res = init()
  res = [0 0 0 0];
  range = [2 1e-11 0.005 185; 10 1e-9 0.05 190];

  for i = 1:length(res)
    res(i) = randSample(range(1, i), range(2, i));
  end

function sample = randSample(lower, upper)
  sample = rand(1,1)*(upper - lower) + lower;
