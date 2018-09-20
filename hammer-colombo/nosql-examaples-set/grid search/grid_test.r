x=c(0.5,  0.52, 0.55, 0.57, 0.6, 0.62, 0.65, 0.68, 0.7,  0.72,  0.75, 0.78, 0.8, 0.825, 0.85, 0.87, 0.88, 0.89, 0.9)
y=c(0.25, 0.27, 0.28, 0.3,  0.3, 0.35, 0.4, 0.45, 0.52, 0.65,  0.74, 0.9, 0.99,   1,    1,    0.9,  0.75, 0.33, 0)
#fit first degree polynomial equation:
fit  <- lm(y~x)
#second degree
fit2 <- lm(y~poly(x,2,raw=TRUE))
#third degree
fit3 <- lm(y~poly(x,3,raw=TRUE))
#fourth degree
fit4 <- lm(y~poly(x,4,raw=TRUE))
#generate range of 50 numbers starting from 30 and ending at 160
xx <- seq(0,1, by=0.001)
plot(x,y,pch=19,ylim=c(-0.2,1.3), xlab="th_sim", ylab="precision")
lines(xx, predict(fit, data.frame(x=xx)), col="red")
#lines(xx, predict(fit2, data.frame(x=xx)), col="green")
lines(xx, predict(fit3, data.frame(x=xx)), col="blue")
#lines(xx, predict(fit4, data.frame(x=xx)), col="purple")