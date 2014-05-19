function fig2eps(filename, width, aspect, inset)

% set(0,'defaulttextinterpreter','latex')
% fig2eps('test.eps',88,1/sqrt(2),[10 10]) 

pt = 72/25.4;                                     % points per mm
dpi = 1200;

if(length(inset) == 4)
  urinset = inset(3:4);
  inset = inset(1:2);
else
  urinset = [0 0];
end



fsize = [width aspect*width];                     % mm
%inset = [10 10];                                  % mm

fsize = floor(fsize*pt);                          % pts
inset = floor(inset*pt);                          % pts
lw = get(gca,'LineWidth');                        % pts

asize = fsize - inset - urinset - lw/2;                      % pts


set(gcf,'PaperUnits','points')
set(gcf,'PaperSize',fsize)
set(gcf,'PaperPosition',[0 0 fsize+[1 0]])        % why is this needed?

set(gca, 'Units', 'points')
set(gca,'Position', [inset asize]);

% IMPORTANT:

%print('-depsc2 ', filename, '-adobecset','-loose','-r1200')
print('-depsc2 ', filename,'-loose','-r1200')


