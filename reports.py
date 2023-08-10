#import findspark
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
#findspark.init()

from pyspark.sql import SparkSession
spark=SparkSession.builder.getOrCreate()
sc=spark.sparkContext
sc


#--------------------------------------------------------------------------------------------------------------------
#read csv files & create sql table

#Aisle table
aisles = spark.read.csv("data/aisles.csv",inferSchema=True,header=True)
aisles.createOrReplaceTempView("aislesTable")

#Departments table
departments = spark.read.csv("data/departments.csv",inferSchema=True,header=True)
#departments.show()
#departments.printSchema() 
departments.createOrReplaceTempView("departmentsTable") 
#spark.sql("select * from departmentsTable").show()      

#Order_Products table
order_products = spark.read.csv("data/order_products.csv",inferSchema=True,header=True)
order_products.createOrReplaceTempView("order_productsTable")

#Orders Table
orders = spark.read.csv("data/orders.csv",inferSchema=True,header=True)
orders.createOrReplaceTempView("ordersTable")

#Products Table
products = spark.read.csv("data/products.csv",inferSchema=True,header=True)
products.createOrReplaceTempView("productsTable")


#--------------------------------------------------------------------------------------------------------------------
# 1. Να παράγει αναφορά με τον συνολικό αριθμό των προϊόντων ανα τμήμα της μορφής «Ονομα_τμήματος, Αριθμός_προϊόντων». 
#Η αναφορά να είναι ταξινομημένη με το όνομα του τμήματος σε αλφαβητική σειρά.

print("1st Query ")
first = spark.sql("select d.department, count(product_id) as sumCount from productsTable as p,departmentsTable as d \
                   where p.department_id=d.department_id group by d.department order by d.department")
first.show(21) #there are 21 departments

#create a report file
q1=first.toPandas()
q1.to_csv("data/reports_plots/q1.csv",index=False)


#--------------------------------------------------------------------------------------------------------------------
#2. Να παράγει αναφορά της μορφής «Ημέρα_εβδομάδας, Αριθμός_Παραγγελιών», με τον συνολικο αριθμό των παραγγγελιών 
#ανα ημέρα της εβδομάδας. Η αναφορά να είναι ταξινομημένη με βάση την ημέρα σε αύξουσα διάταξη.

print("2nd Query")
second = spark.sql("select order_dow as day_of_week, count(order_id) as count_orders from ordersTable group by order_dow order by order_dow")
second.show()

#create a report file
q2=second.toPandas()
q2.to_csv("data/reports_plots/q2.csv",index=False)


#--------------------------------------------------------------------------------------------------------------------
#3. Να παραγει αναφορά με τα προϊόντα ανα τμήμα που δεν έχουν παραγγελθεί παραπάνω από μία φορά από κανέναν πελάτη. 
#Η αναφορά πρέπει να έχει την μορφή «Ονομα_τμήματος, Κωδικός_προϊόντος, ονομα_Προϊόντος» και να είναι ταξινομημένη 
#αλφαβητικά με το όνομα του τμήματος και το όνομα του προϊόντος.


print("3rd Query")
third = spark.sql("select d.department, p.product_id ,p.product_name \
                  from departmentsTable as d,productsTable as p,order_productsTable as o \
                  where d.department_id=p.department_id and p.product_id=o.product_id \
                  group by department,p.product_id ,product_name \
                  having sum(reordered)=0\
                  order by d.department,p.product_name")
third.show()


#create a report file
q3=third.toPandas()
q3.to_csv("data/reports_plots/q3.csv",index=False)


#--------------------------------------------------------------------------------------------------------------------
#4. Να παράγει αναφορά με το όνομα του προϊόντος κάθε τμήματος που έχει παραγγλεθεί επανελειμμένα τις περισσότερες
#φορές με την εξής μορφή: «Ονομα_τμήματος, Ονομα_προϊόντος, Φορές_που_έχει_παραγγελθεί». 
#Η αναφορά να είναι ταξινομημένη αλφαβητικά με το όνομα του τμήματος.

print("4th Query")
forth= spark.sql("select department,product_name,count(reordered) as times_ordered \
                 from departmentsTable as d, productsTable as p, order_productsTable as o\
                 where d.department_id=p.department_id and p.product_id=o.product_id and reordered=1 \
                 group by department,product_name\
                 order by department, times_ordered desc")
#forth.show()

#εντολή για αφαίρεση duplicates ώστε να βρούμε το μοναδικό product_name (το max)
forth_2 = forth.drop_duplicates(subset=['department'])
forth_2.show()

#create a report file
q4=forth_2.toPandas()
q4.to_csv("data/reports_plots/q4.csv",index=False)


#--------------------------------------------------------------------------------------------------------------------
#5. Να παράγει αναφορά της μορφής «Ονομα διαδρόμου, Ποσοστό», με το ποσοστό των προϊόντων κάθε διαδρόμου που έχουν 
#τοποθετηθεί πρώτα στο καλάθι κάποιας παραγγελίας. Η αναφορά να είναι ταξινομημένη αλφαβητικά με το όνομα του διαδρόμου. 

print("5th Query")

print("Ypologismos arithmiti")
#arithmitis posostou = proionta kathe diadromoy poy exoyn toopothetithei prwta sto kalathi
temp1 = spark.sql("select aisle_id,count(*) as count_products\
                    from (select o.product_id,a.aisle_id \
                        from aislesTable as a,order_productsTable as o,productsTable as p\
                        where a.aisle_id=p.aisle_id and p.product_id=o.product_id and add_to_cart_order=1\
                        group by o.product_id,a.aisle_id order by a.aisle_id)\
                    group by aisle_id order by aisle_id")
temp1.show()
temp1.createOrReplaceTempView("arithmitis")

print("Ypologismos paronomasti")
#paronomastis posostoy = #proionta kathe diadromoy
temp2=spark.sql("select aisle_id,count(product_id) as total_count_products from productsTable group by aisle_id order by aisle_id")
temp2.show()
temp2.createOrReplaceTempView("paronomastis")

print("Teliko apotelesma 5th query")
#pososto = arithmitis/paronomastis
fifth = spark.sql("select a.aisle,  round(100*count_products/total_count_products,2) as products_aisle_OneTimeOrdered  \
                  from arithmitis as ar,paronomastis as pa,aislesTable as a\
                  where pa.aisle_id=ar.aisle_id and ar.aisle_id=a.aisle_id \
                  order by a.aisle")
fifth.show()

#create a report file
q5=fifth.toPandas()
q5.to_csv("data/reports_plots/q5.csv",index=False)


#--------------------------------------------------------------------------------------------------------------------
#6. Να δημιουργεί κατάλληλα γραφήματα (π.χ. Ηistogram, Pie chart) για την παρουσίαση των περιεχομένων της πρώτης
#και της δεύτερης αναφοράς (βλέπε 1 και 2).

print("6th Query")
print("check .png files")
#create plot for 1st report
q1.plot(kind="bar")
plt.xlabel("departments")
plt.ylabel("count of products")
plt.title("Query 1")
plt.savefig("data/reports_plots/q1_plot.png")

#create plot for 2nd report
q2.plot(kind="barh")
plt.xlabel("day of week")
plt.ylabel("count of orders")
plt.title("Query 2")
plt.savefig("data/reports_plots/q2_plot.png")
