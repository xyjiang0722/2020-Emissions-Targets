library(data.table)
library(ggplot2)

data = fread("figures/regression_coefficients_announcement.csv")

data[,order:=33-3*order]
y_labels = data$depvar

ggplot(data, aes(x = b, y = order, colour = factor(indepvar))) +
  geom_point() +
  geom_errorbarh(aes(xmin = lower, xmax = upper), height = 0.5) +
  geom_vline(aes(xintercept = 0), colour = "red", linetype = "longdash") +
  xlab("Estimate with standard error") + ylab("") + xlim(-0.75, 0.5) +
  scale_y_continuous(breaks=data$order, labels = y_labels) +
  scale_colour_manual(labels = c("Post"), values = c("#F8766D")) + 
  theme(legend.title = element_blank(), legend.position = "bottom", 
        legend.key=element_blank(),
        panel.grid.major.y = element_blank(), panel.grid.minor.y = element_blank(),
        axis.ticks.y = element_blank(), panel.background = element_blank(),
        axis.line.x = element_line(), axis.ticks.x = element_line())
ggsave("figures/regression_coefficients_announcement.png", width = 8, height = 6, dpi = 300)




