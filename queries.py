# pylint:disable=C0111,C0103
import sqlite3
conn = sqlite3.connect('data/ecommerce.sqlite')
c = conn.cursor()

def order_rank_per_customer(db):
# - Implement `order_rank_per_customer` to rank the orders of each customer according to the order date.
# - For each customer, the orders should be ranked in the chronological order.
# - This function should return a list of tuples like (`OrderID`, `CustomerID`, `OrderDate`, `OrderRank`).
    query = """SELECT Orders.OrderID, orders.CustomerID , orders.OrderDate,
    RANK() OVER (
    PARTITION BY orders.CustomerID
    ORDER BY orders.OrderDate
    ) AS 'order_rank_per_customer'
    FROM Orders
    """
    c.execute(query)
    rows = c.fetchall()
    return rows




def order_cumulative_amount_per_customer(db):
# - Implement `order_cumulative_amount_per_customer` to compute the cumulative amount (in USD) of the orders of each customer according to the order date.
# - For each customer, the orders should be ranked in the chronological order.
# - This function should return a list of tuples like (`OrderID`, `CustomerID`, `OrderDate`, `OrderCumulativeAmount`).
    query = """
    SELECT DISTINCT orders.OrderID, orders.CustomerID , orders.OrderDate,
    SUM(OrderDetails.UnitPrice * OrderDetails.Quantity) OVER(
    PARTITION BY orders.CustomerID
    ORDER BY orders.OrderDate
    ) AS 'order_cumulative_amount_per_customer'
    FROM Orders
    LEFT JOIN OrderDetails ON OrderDetails.OrderID = Orders.OrderID
    """
    c.execute(query)
    rows = c.fetchall()
    return rows
