install.packages("data.table")
install.packages("ggplot2")

library(data.table)
library(ggplot2)

data = fread("figures/regression_coefficients_main.csv")
data[,order:=order1*9+order2*2]
data[,order:=101-order]
data[,depvar:=ifelse(order2==2,depvar,"")]

y_labels = data$depvar

ggplot(data, aes(x = b_, y = order, colour = factor(order2))) +
  geom_point() +
  geom_errorbarh(aes(xmin = lower_, xmax = upper_)) +
  geom_vline(aes(xintercept = 0), colour = "black", linetype = "longdash") +
  xlab("Estimate with standard error") + ylab("") + xlim(-0.75, 1.25) +
  scale_y_continuous(breaks=data$order, labels = y_labels) +
  scale_colour_manual(labels = c("Failed", "Disappeared - leaders", "Disappeared - laggards"),
                      values = c("#809bc8", "#997700", "#ee7733")) + 
  theme(legend.title = element_blank(), legend.position = "bottom",  
        legend.key=element_blank(), # legend.background = element_rect(fill="#F5F5F5"),
        panel.grid.major.y = element_blank(), panel.grid.minor.y = element_blank(),
        axis.ticks.y = element_blank(), panel.background = element_blank(),
        axis.line.x = element_line(), axis.ticks.x = element_line())
ggsave("figures/figure_3.png", width = 180, height = 140, dpi = 500, unit="mm")

