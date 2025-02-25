# Title: "Build Triggers in SQLite"
# Author: "Sai Karthikeyan, Sura"
# Date: "02/25/2025"

# Step 1: Load RSQLite library
library(RSQLite)

# Step 2: Connect to the SQLite database
dbCon <- dbConnect(RSQLite::SQLite(), "OrdersDB.sqlitedb.db")

# Function to check if a column exists in the Employees table
column_exists <- function(column_name) {
  columns <- dbGetQuery(dbCon, "PRAGMA table_info(Employees);")
  return(column_name %in% columns$name)
}

# Step 3: Alter the Employees table to add the TotalSold column
if (!column_exists("TotalSold")) {
  dbExecute(dbCon, "ALTER TABLE Employees ADD COLUMN TotalSold NUMERIC CHECK (TotalSold >= 0);")
}

# Step 4: Update TotalSold column with the total revenue per employee
dbExecute(dbCon, "UPDATE Employees
SET TotalSold = COALESCE((
    SELECT SUM(od.Quantity * p.Price)
    FROM Orders o
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    WHERE o.EmployeeID = Employees.EmployeeID
), 0);")

# Function to check if the trigger exists in the sqlite_master table
trigger_exists <- function(trigger_name) {
  result <- dbGetQuery(dbCon, sprintf("SELECT name FROM sqlite_master WHERE type='trigger' AND name='%s';", trigger_name))
  return(nrow(result) > 0)
}

# Step 5: Create an AFTER INSERT on OrderDetails
if (!trigger_exists("update_totalsold_after_insert")) {
  dbExecute(dbCon, "CREATE TRIGGER update_totalsold_after_insert
  AFTER INSERT ON OrderDetails
  FOR EACH ROW
  BEGIN
      UPDATE Employees
      SET TotalSold = COALESCE((
          SELECT SUM(od.Quantity * p.Price)
          FROM Orders o
          JOIN OrderDetails od ON o.OrderID = od.OrderID
          JOIN Products p ON od.ProductID = p.ProductID
          WHERE o.EmployeeID = Employees.EmployeeID
      ), 0)
      WHERE EmployeeID IN (
          SELECT EmployeeID FROM Orders WHERE OrderID = NEW.OrderID
      );
  END;")
}

# Step 6: Create an AFTER UPDATE trigger on OrderDetails
if (!trigger_exists("update_totalsold_after_update")) {
  dbExecute(dbCon, "CREATE TRIGGER update_totalsold_after_update
  AFTER UPDATE ON OrderDetails
  FOR EACH ROW
  BEGIN
      UPDATE Employees
      SET TotalSold = COALESCE((
          SELECT SUM(od.Quantity * p.Price)
          FROM Orders o
          JOIN OrderDetails od ON o.OrderID = od.OrderID
          JOIN Products p ON od.ProductID = p.ProductID
          WHERE o.EmployeeID = Employees.EmployeeID
      ), 0)
      WHERE EmployeeID IN (
          SELECT EmployeeID FROM Orders WHERE OrderID = NEW.OrderID
      );
  END;")
}

# Step 7: Create an AFTER DELETE trigger on OrderDetails
if (!trigger_exists("update_totalsold_after_delete")) {
  dbExecute(dbCon, "CREATE TRIGGER update_totalsold_after_delete
  AFTER DELETE ON OrderDetails
  FOR EACH ROW
  BEGIN
      UPDATE Employees
      SET TotalSold = COALESCE((
          SELECT SUM(od.Quantity * p.Price)
          FROM Orders o
          JOIN OrderDetails od ON o.OrderID = od.OrderID
          JOIN Products p ON od.ProductID = p.ProductID
          WHERE o.EmployeeID = Employees.EmployeeID
      ), 0)
      WHERE EmployeeID IN (
          SELECT EmployeeID FROM Orders WHERE OrderID = OLD.OrderID
      );
  END;")
}

# Step 8: Disconnect from the database
dbDisconnect(dbCon)
