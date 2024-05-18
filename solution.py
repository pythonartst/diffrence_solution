from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, collect_list
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType


@udf(returnType=ArrayType(StructType([\
    StructField("initial_balance", IntegerType(), True),
    StructField("available_balance", IntegerType(), True),
    StructField("validation_result", StringType(), True),
    StructField("status", StringType(), True),
    StructField("balance_order", IntegerType(), True)
]))) 
def rules_and_diffrence_udf(list_balance_order, list_available_balance,
                            list_withdraw_order, list_withdraw_amount, li_status):
    #balance_order   initial_balance available_balance   status  validation_result
    
    withdrawal = zip(list_withdraw_order, list_withdraw_amount)  # no need to sort as we already did order by
    ans = []
    
    for w_order, w_amt in withdrawal:
        total_funds = sum(list_available_balance)
        if w_amt> total_funds:
            for idx, b_amt in enumerate(list_available_balance):
                cur_ans = {}
                cur_ans['initial_balance']   = b_amt
                cur_ans['available_balance'] = b_amt
                cur_ans['validation_result'] = f"Withdrawal not happend of ${w_amt}, Insufficient funds, total funds = {total_funds}"
                cur_ans['status']            = li_status[idx] if b_amt != 0 else 'BALANCE WITHDREW'
                cur_ans['balance_order']     = list_balance_order[idx]
                ans.append(cur_ans)
            continue
        updated_balance_li = []
        for idx, b_amt in enumerate(list_available_balance):
            cur_ans = {}
            dols = 0
            if b_amt>=w_amt:
                cur_bal = b_amt-w_amt
                dols = w_amt
                cur_ans['status']            = li_status[idx] if b_amt != 0 else 'BALANCE WITHDREW'
            else:
                cur_bal = 0
                w_amt =w_amt - b_amt
                cur_ans['status']            = f"BALANCE WITHDREW"
                dols = b_amt

            
            cur_ans['initial_balance']   = b_amt
            cur_ans['available_balance'] = cur_bal
            cur_ans['validation_result'] = f"Withdrawal Happend, of ${dols}"
            
            cur_ans['balance_order']     = list_balance_order[idx]
            ans.append(cur_ans)
            updated_balance_li.append(cur_bal)
        list_available_balance = updated_balance_li
        
    return ans


def solve(spark, rules_and_diffrence_udf):
    # account_id	balance_order	available_balance	status
    balance_df = spark.read.csv("balance.csv", header=True, inferSchema=True)

    # account_id	withdraw_amount	withdraw_order
    withdraw_df = spark.read.csv("withdraw.csv", header=True, inferSchema=True)
    
    
    

    agg_balance_df = balance_df.orderBy('account_id', 'balance_order').groupBy("account_id").agg(collect_list("balance_order").alias("list_balance_order"),
                                                          collect_list("available_balance").alias("list_available_balance"),
                                                          collect_list("status").alias("list_status"))
    

    agg_withdraw_df = withdraw_df.orderBy('account_id', 'withdraw_order').groupBy("account_id").agg(collect_list("withdraw_amount").alias("list_withdraw_amount"),
                                                          collect_list("withdraw_order").alias("list_withdraw_order"))
    
    
    
    joined_df = agg_balance_df.join(agg_withdraw_df, on='account_id', how='left')
    joined_df_with_final_data = joined_df.withColumn('final_data', rules_and_diffrence_udf(joined_df['list_balance_order'],
                                                                                           joined_df['list_available_balance'],
                                                                                           joined_df['list_withdraw_order'],
                                                                                           joined_df['list_withdraw_amount'],
                                                                                           joined_df['list_status'])
                                                                                           )
    
    final_df = joined_df_with_final_data.select('account_id', 'final_data').withColumn('efc', explode('final_data'))

    final_df = final_df.select('account_id', 'efc.balance_order','efc.initial_balance', 'efc.available_balance','efc.status', 'efc.validation_result')
    return final_df




if __name__ == '__main__':
    
    spark = SparkSession.builder.appName("Diffrenece_game").getOrCreate()
    df = solve(spark, rules_and_diffrence_udf)
    
    # df.coalesce(1).write.csv('output_balance', header = True, mode='overwrite')
    df.orderBy('account_id').show(n=df.count(), truncate=False)
    
