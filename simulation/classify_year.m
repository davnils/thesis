function result=classifyYear()
  clear
  for i=1:365
    file = sprintf('output_%i', i);
    run(file)
    % voltage (0): 0.3
    % current (1): 0.6
    res = detect(output_', 1, 24, 0, 0.3);

    if any(any(res)) == 1
      i
    end
  end
