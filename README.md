# tbus
Шина сообщений вида publisher-subscriber, направленная через транзакционную БД. Используется при необходимости рассылать только закомиченные изменения в рамках транзакции. События сначала поступают в БД в рамках некой внешней транзакции, и отправляются подписчикам только после комита.

Реализована поверх https://github.com/n-r-w/mbus
