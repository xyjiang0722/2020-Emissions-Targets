library(data.table)
library(ggplot2)


### Prior information
data = fread("figures/regression_coefficients_prior_info.csv")
data[,order:=order1*9+order2*2]

temp = data[,.(order=mean(order)), keyby=.(order1,depvar)]
data = rbind(data, temp, fill=T)
setorder(data, order)

data[,order:=101-order]
data[,depvar:=ifelse(is.na(order2),depvar,"")]

y_labels = data$depvar

ggplot(data, aes(x = b_, y = order, colour = factor(order2))) +
  geom_point() +
  geom_errorbarh(aes(xmin = lower_, xmax = upper_)) +
  geom_vline(aes(xintercept = 0), colour = "black", linetype = "longdash") +
  xlab("Estimate with standard error") + ylab("") + xlim(-1.5, 2.0) +
  scale_y_continuous(breaks=data$order, labels = y_labels) +
  scale_colour_manual(labels = c("Lagging Behind in 2018", "Lagging Behind in 2019", ""),
                      values = c("#ee99aa", "#994455", "")) + 
  theme(legend.title = element_blank(), legend.position = "bottom", 
        legend.key=element_blank(),
        panel.grid.major.y = element_blank(), panel.grid.minor.y = element_blank(),
        axis.ticks.y = element_blank(), panel.background = element_blank(),
        axis.line.x = element_line(), axis.ticks.x = element_line())  +
  guides(colour = guide_legend(override.aes = list(colour = c("#ee99aa", "#994455", NA))))
ggsave("figures/figure_4a.png", width = 180, height = 140, dpi = 500, unit="mm")



### Taregt ambition
data = fread("figures/regression_coefficients_ambitiousness.csv")
data[,order:=order1*9+order2*2]

temp = data[,.(order=mean(order)), keyby=.(order1,depvar)]
data = rbind(data, temp, fill=T)
setorder(data, order)

data[,order:=101-order]
data[,depvar:=ifelse(is.na(order2),depvar,"")]

y_labels = data$depvar

ggplot(data, aes(x = b_, y = order, colour = factor(order2))) +
  geom_point() +
  geom_errorbarh(aes(xmin = lower_, xmax = upper_)) +
  geom_vline(aes(xintercept = 0), colour = "black", linetype = "longdash") +
  xlab("Estimate with standard error") + ylab("") + xlim(-1.0, 1.5) +
  scale_y_continuous(breaks=data$order, labels = y_labels) +
  scale_colour_manual(labels = c("Failed - Ambitious", "Failed - Unambitious", ""),
                      values = c("#33bbee", "#0077bb", "")) + 
  theme(legend.title = element_blank(), legend.position = "bottom", 
        legend.key=element_blank(),
        panel.grid.major.y = element_blank(), panel.grid.minor.y = element_blank(),
        axis.ticks.y = element_blank(), panel.background = element_blank(),
        axis.line.x = element_line(), axis.ticks.x = element_line())  +
  guides(colour = guide_legend(override.aes = list(colour = c("#33bbee", "#0077bb", NA))))
ggsave("figures/figure_4b.png", width = 180, height = 140, dpi = 500, unit="mm")



### COVID
data = fread("figures/regression_coefficients_covid.csv")
data[,order:=order1*9+order2*2]
data[,order:=101-order]
data[,depvar:=ifelse(order2==2,depvar,"")]

y_labels = data$depvar

ggplot(data, aes(x = b_, y = order, colour = factor(order2))) +
  geom_point() +
  geom_errorbarh(aes(xmin = lower_, xmax = upper_)) +
  geom_vline(aes(xintercept = 0), colour = "black", linetype = "longdash") +
  xlab("Estimate with standard error") + ylab("") + xlim(-1.5, 1.25) +
  scale_y_continuous(breaks=data$order, labels = y_labels) +
  scale_colour_manual(labels = c("Failed", "Disappeared - leaders", "Disappeared - laggards"),
                      values = c("#809bc8", "#997700", "#ee7733")) + 
  theme(legend.title = element_blank(), legend.position = "bottom", 
        legend.key=element_blank(),
        panel.grid.major.y = element_blank(), panel.grid.minor.y = element_blank(),
        axis.ticks.y = element_blank(), panel.background = element_blank(),
        axis.line.x = element_line(), axis.ticks.x = element_line())
ggsave("figures/figure_4c.png", width = 180, height = 140, dpi = 500, unit="mm")



### Materiality
data = fread("figures/regression_coefficients_materiality.csv")
data[,order:=order1*9+order2*2]
data[,order:=101-order]
data[,depvar:=ifelse(order2==2,depvar,"")]

y_labels = data$depvar

ggplot(data, aes(x = b_, y = order, colour = factor(order2))) +
  geom_point() +
  geom_errorbarh(aes(xmin = lower_, xmax = upper_)) +
  geom_vline(aes(xintercept = 0), colour = "black", linetype = "longdash") +
  xlab("Estimate with standard error") + ylab("") + xlim(-1.25, 1.25) +
  scale_y_continuous(breaks=data$order, labels = y_labels) +
  scale_colour_manual(labels = c("Failed", "Disappeared - leaders", "Disappeared - laggards"),
                      values = c("#809bc8", "#997700", "#ee7733")) + 
  theme(legend.title = element_blank(), legend.position = "bottom",  
        legend.key=element_blank(),
        panel.grid.major.y = element_blank(), panel.grid.minor.y = element_blank(),
        axis.ticks.y = element_blank(), panel.background = element_blank(), 
        axis.line.x = element_line(), axis.ticks.x = element_line())
ggsave("figures/figure_4d.png", width = 180, height = 140, dpi = 500, unit="mm")

