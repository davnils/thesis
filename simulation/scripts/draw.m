function draw(xvec, yvec, c1, c2, c3, cn)
  clf;
  k = 2;
  imagesc(hist3([-yvec;xvec]', [128*k 128*k]));
  hold all;

  plot(c1(:,1), c1(:,2), '.', 'MarkerSize', 70);
  plot(c1(:,1), c1(:,2), '.', 'MarkerSize', 70);
  plot(c2(:,1), c2(:,2), '.', 'MarkerSize', 70);
  plot(c3(:,1), c3(:,2), '.', 'MarkerSize', 70);
  % plot(c4(:,1), c4(:,2), '.', 'MarkerSize', 70);
  plot(cn(:,1), cn(:,2), '.', 'MarkerSize', 70);
