spark-submit solution.py

+----------+-------------+---------------+-----------------+----------------+---------------------------------------------------------------------------+
|account_id|balance_order|initial_balance|available_balance|status          |validation_result                                                          |
+----------+-------------+---------------+-----------------+----------------+---------------------------------------------------------------------------+
|1         |1            |80006          |7744             |ACTIVE          |Withdrawal Happend, of $72262                                              |
|2         |1            |11531          |4665             |ACTIVE          |Withdrawal Happend, of $6866                                               |
|3         |1            |80457          |12045            |ACTIVE          |Withdrawal Happend, of $68412                                              |
|4         |1            |11277          |11277            |ACTIVE          |Withdrawal not happend of $14251, Insufficient funds, total funds = 11277  |
|5         |1            |84814          |29395            |ACTIVE          |Withdrawal Happend, of $55419                                              |
|5         |2            |76616          |21197            |ACTIVE          |Withdrawal Happend, of $55419                                              |
|5         |3            |58400          |2981             |ACTIVE          |Withdrawal Happend, of $55419                                              |
|5         |1            |29395          |0                |BALANCE WITHDREW|Withdrawal Happend, of $29395                                              |
|5         |2            |21197          |0                |BALANCE WITHDREW|Withdrawal Happend, of $21197                                              |
|5         |3            |2981           |2667             |ACTIVE          |Withdrawal Happend, of $314                                                |
|6         |1            |5831           |5831             |ACTIVE          |Withdrawal not happend of $46443, Insufficient funds, total funds = 5831   |
|7         |1            |44663          |40174            |ACTIVE          |Withdrawal Happend, of $4489                                               |
|8         |1            |38693          |38693            |ACTIVE          |Withdrawal not happend of $64021, Insufficient funds, total funds = 38693  |
|9         |1            |68406          |55258            |ACTIVE          |Withdrawal Happend, of $13148                                              |
|10        |1            |18881          |18881            |ACTIVE          |Withdrawal not happend of $98145, Insufficient funds, total funds = 18881  |
|11        |1            |67584          |67584            |ACTIVE          |Withdrawal not happend of $86807, Insufficient funds, total funds = 67584  |
|12        |1            |58171          |0                |BALANCE WITHDREW|Withdrawal Happend, of $58171                                              |
|12        |2            |61429          |51428            |ACTIVE          |Withdrawal Happend, of $10001                                              |
|12        |1            |0              |0                |BALANCE WITHDREW|Withdrawal not happend of $86858, Insufficient funds, total funds = 51428  |
|12        |2            |51428          |51428            |ACTIVE          |Withdrawal not happend of $86858, Insufficient funds, total funds = 51428  |
|13        |1            |96451          |82663            |ACTIVE          |Withdrawal Happend, of $13788                                              |
|14        |1            |65361          |38183            |ACTIVE          |Withdrawal Happend, of $27178                                              |
|15        |1            |12097          |0                |BALANCE WITHDREW|Withdrawal Happend, of $12097                                              |
|15        |2            |97167          |0                |BALANCE WITHDREW|Withdrawal Happend, of $97167                                              |
|15        |3            |59323          |12586            |ACTIVE          |Withdrawal Happend, of $46737                                              |
|15        |4            |98478          |51741            |ACTIVE          |Withdrawal Happend, of $46737                                              |
|15        |5            |87080          |40343            |ACTIVE          |Withdrawal Happend, of $46737                                              |
|15        |1            |0              |0                |BALANCE WITHDREW|Withdrawal Happend, of $0                                                  |
|15        |2            |0              |0                |BALANCE WITHDREW|Withdrawal Happend, of $0                                                  |
|15        |3            |12586          |0                |BALANCE WITHDREW|Withdrawal Happend, of $12586                                              |
|15        |4            |51741          |0                |BALANCE WITHDREW|Withdrawal Happend, of $51741                                              |
|15        |5            |40343          |33214            |ACTIVE          |Withdrawal Happend, of $7129                                               |
|15        |1            |0              |0                |BALANCE WITHDREW|Withdrawal not happend of $300000, Insufficient funds, total funds = 33214 |
|15        |2            |0              |0                |BALANCE WITHDREW|Withdrawal not happend of $300000, Insufficient funds, total funds = 33214 |
|15        |3            |0              |0                |BALANCE WITHDREW|Withdrawal not happend of $300000, Insufficient funds, total funds = 33214 |
|15        |4            |0              |0                |BALANCE WITHDREW|Withdrawal not happend of $300000, Insufficient funds, total funds = 33214 |
|15        |5            |33214          |33214            |ACTIVE          |Withdrawal not happend of $300000, Insufficient funds, total funds = 33214 |
|16        |1            |43274          |864              |ACTIVE          |Withdrawal Happend, of $42410                                              |
|17        |1            |68427          |68427            |ACTIVE          |Withdrawal not happend of $92540, Insufficient funds, total funds = 84667  |
|17        |2            |16240          |16240            |ACTIVE          |Withdrawal not happend of $92540, Insufficient funds, total funds = 84667  |
|17        |1            |68427          |42768            |ACTIVE          |Withdrawal Happend, of $25659                                              |
|17        |2            |16240          |0                |BALANCE WITHDREW|Withdrawal Happend, of $16240                                              |
|18        |1            |20823          |20823            |ACTIVE          |Withdrawal not happend of $96368, Insufficient funds, total funds = 20823  |
|19        |1            |92009          |69907            |ACTIVE          |Withdrawal Happend, of $22102                                              |
|20        |1            |46085          |23006            |ACTIVE          |Withdrawal Happend, of $23079                                              |
|21        |1            |22611          |13175            |ACTIVE          |Withdrawal Happend, of $9436                                               |
|22        |1            |96815          |37814            |ACTIVE          |Withdrawal Happend, of $59001                                              |
|23        |1            |51767          |51767            |ACTIVE          |Withdrawal not happend of $56668, Insufficient funds, total funds = 51767  |
|24        |1            |3821           |3821             |ACTIVE          |Withdrawal not happend of $78162, Insufficient funds, total funds = 3821   |
|25        |1            |57297          |24745            |ACTIVE          |Withdrawal Happend, of $32552                                              |
|26        |1            |69406          |69406            |ACTIVE          |Withdrawal not happend of $88876, Insufficient funds, total funds = 69406  |
|27        |1            |31713          |31713            |ACTIVE          |Withdrawal not happend of $45665, Insufficient funds, total funds = 31713  |
|28        |1            |30431          |30431            |ACTIVE          |Withdrawal not happend of $96036, Insufficient funds, total funds = 30431  |
|29        |1            |81676          |61193            |ACTIVE          |Withdrawal Happend, of $20483                                              |
|30        |1            |30992          |0                |BALANCE WITHDREW|Withdrawal Happend, of $30992                                              |
|30        |2            |37556          |33888            |ACTIVE          |Withdrawal Happend, of $3668                                               |
|30        |3            |94518          |90850            |ACTIVE          |Withdrawal Happend, of $3668                                               |
|30        |4            |46640          |42972            |ACTIVE          |Withdrawal Happend, of $3668                                               |
|30        |1            |0              |0                |BALANCE WITHDREW|Withdrawal Happend, of $0                                                  |
|30        |2            |33888          |0                |BALANCE WITHDREW|Withdrawal Happend, of $33888                                              |
|30        |3            |90850          |49157            |ACTIVE          |Withdrawal Happend, of $41693                                              |
|30        |4            |42972          |1279             |ACTIVE          |Withdrawal Happend, of $41693                                              |
|30        |1            |0              |0                |BALANCE WITHDREW|Withdrawal Happend, of $0                                                  |
|30        |2            |0              |0                |BALANCE WITHDREW|Withdrawal Happend, of $0                                                  |
|30        |3            |49157          |36766            |ACTIVE          |Withdrawal Happend, of $12391                                              |
|30        |4            |1279           |0                |BALANCE WITHDREW|Withdrawal Happend, of $1279                                               |
|30        |1            |0              |0                |BALANCE WITHDREW|Withdrawal not happend of $161562, Insufficient funds, total funds = 36766 |
|30        |2            |0              |0                |BALANCE WITHDREW|Withdrawal not happend of $161562, Insufficient funds, total funds = 36766 |
|30        |3            |36766          |36766            |ACTIVE          |Withdrawal not happend of $161562, Insufficient funds, total funds = 36766 |
|30        |4            |0              |0                |BALANCE WITHDREW|Withdrawal not happend of $161562, Insufficient funds, total funds = 36766 |
|31        |1            |50496          |1079             |ACTIVE          |Withdrawal Happend, of $49417                                              |
|32        |1            |82804          |82804            |ACTIVE          |Withdrawal not happend of $94626, Insufficient funds, total funds = 82804  |
|33        |1            |7077           |7077             |ACTIVE          |Withdrawal not happend of $96948, Insufficient funds, total funds = 7077   |
|34        |1            |82988          |37134            |ACTIVE          |Withdrawal Happend, of $45854                                              |
|35        |1            |77632          |49975            |ACTIVE          |Withdrawal Happend, of $27657                                              |
|35        |2            |47406          |19749            |ACTIVE          |Withdrawal Happend, of $27657                                              |
|35        |3            |49196          |21539            |ACTIVE          |Withdrawal Happend, of $27657                                              |
|35        |4            |44460          |16803            |ACTIVE          |Withdrawal Happend, of $27657                                              |
|35        |1            |49975          |49975            |ACTIVE          |Withdrawal not happend of $404460, Insufficient funds, total funds = 108066|
|35        |2            |19749          |19749            |ACTIVE          |Withdrawal not happend of $404460, Insufficient funds, total funds = 108066|
|35        |3            |21539          |21539            |ACTIVE          |Withdrawal not happend of $404460, Insufficient funds, total funds = 108066|
|35        |4            |16803          |16803            |ACTIVE          |Withdrawal not happend of $404460, Insufficient funds, total funds = 108066|
+----------+-------------+---------------+-----------------+----------------+---------------------------------------------------------------------------+
