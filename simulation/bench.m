function res = bench(runs)
  xl= [2 1e-12 1e-3 180];
  xu = [10 1e-7 1e-1 190];
  options = optimset('TolFun',1e-16, 'TolX', 1e-16, 'MaxFunEvals', 5000, 'MaxIter', 5000);

  solutions = [];

  for i = 1:runs
    [x, norm, resid, exitflag] = lsqnonlin(@wrapper, init(), xl, xu, options);

    if exitflag > 0
      save = [x norm];
      solutions = [solutions save'];
    end
  end

  res = solutions;
