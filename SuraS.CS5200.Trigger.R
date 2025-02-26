# Title: "Build Triggers in SQLite"
# Author: "Sai Karthikeyan, Sura"
# Date: "02/25/2025"

# Load required libraries
installPackages <- function(packages) {
  # install packages not yet installed
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
  # load all packages by applying 'library' function
  invisible(lapply(packages, library, character.only = TRUE))
}

# Connect to the SQLite database
ConnectSQLiteDB <- function(dbName) {
  dbFile = paste0(dbName)
  dbCon <- dbConnect(RSQLite::SQLite(), dbFile)
  return(dbCon)
}

# Alter the Employees table to add the TotalSold column
AddTotalSoldInEmployeesTable <- function(dbCon) {
if ("TotalSold" %in% dbListFields(dbCon, "Employees")) {
  dbExecute(dbCon, "ALTER TABLE Employees DROP COLUMN TotalSold;")
}
dbExecute(dbCon, "ALTER TABLE Employees ADD COLUMN TotalSold NUMERIC;")
}

# Update TotalSold column with the total revenue per employee
updateTotalSoldColumnInEmployeesTable <- function(dbCon) {
dbExecute(dbCon, "UPDATE Employees
SET TotalSold = COALESCE((
    SELECT SUM(od.Quantity * p.Price)
    FROM Orders o
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    JOIN Products p ON od.ProductID = p.ProductID
    WHERE o.EmployeeID = Employees.EmployeeID
), 0);")
}

# Create an AFTER INSERT on OrderDetails
insertTriggerOnOrderDetailsTable <- function(dbCon) {
  dbExecute(dbCon, "DROP TRIGGER IF EXISTS update_totalsold_after_insert;")
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

# Create an AFTER UPDATE trigger on OrderDetails
updateTriggerOnOrderDetailsTable <- function(dbCon) {
  dbExecute(dbCon, "DROP TRIGGER IF EXISTS update_totalsold_after_update;")
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

# Create an AFTER DELETE trigger on OrderDetails
deleteTriggerOnOrderDetailsTable <- function(dbCon) {
  dbExecute(dbCon, "DROP TRIGGER IF EXISTS update_totalsold_after_delete;")
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

# Validating the Triggers
validateInsertTrigger <- function(dbCon) {
  tryCatch({
    cat("Validating insert trigger\n")
    # sample data for validation
    employeeIdToTest <- 3
    productIdToTest <- 3
    orderIdToTest <- 10253
    
    # get the total sold of employee before inserting a row in OrderDetails
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold by the employee with the id %s is %s\n", employeeIdToTest, employee$TotalSold))
    
    # get the price of the product
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of the product for 1 unit is %s\n", productPrice$Price))
    
    # insert a row in OrderDetails with quantity 1
    dbExecute(dbCon, sprintf("INSERT INTO OrderDetails (OrderID, ProductID, Quantity) VALUES (%s, %s, 1);", orderIdToTest, productIdToTest))
    
    # get the total sold of employee after inserting a row in OrderDetails
    employeeAfterInsert <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold by the employee with the id %s after inserting 1 quantity is %s\n", employeeIdToTest, employeeAfterInsert$TotalSold))
    
    # validate the trigger by checking if the TotalSold column in Employees table is updated
    cat(sprintf("Expected increase in price: %s, actual:%s\n", productPrice$Price * 1, employeeAfterInsert$TotalSold - employee$TotalSold))
    test_that("Test Insert Trigger:", {
      expect_equal(employeeAfterInsert$TotalSold - employee$TotalSold, productPrice$Price * 1)
    })
    cat("\n")
  }, error = function(e) {
    cat("Error while validating insert trigger: ", e$message, "\n")
  })
}


validateUpdateTrigger <- function(dbCon) {
  tryCatch({
    cat("Validating update trigger\n")
    # sample data for validation
    employeeIdToTest <- 3
    productIdToTest <- 3
    orderIdToTest <- 10253
    
    # get the total sold of employee before updating a row in OrderDetails
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold by the employee with the id %s is %s\n", employeeIdToTest, employee$TotalSold))
    
    # get the price of the product
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of the product for 1 unit is %s\n", productPrice$Price))
    
    # update the quantity of a row in OrderDetails to 2
    dbExecute(dbCon, sprintf("UPDATE OrderDetails SET Quantity = 2 WHERE OrderID = %s AND ProductID = %s;", orderIdToTest, productIdToTest))
    
    # get the total sold of employee after updating a row in OrderDetails
    employeeAfterUpdate <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold by the employee with the id %s after updating quantity to 2 is %s\n", employeeIdToTest, employeeAfterUpdate$TotalSold))

    # validate the trigger by checking if the TotalSold column in Employees table is updated
    cat(sprintf("Expected increase in price: %s, actual:%s\n", productPrice$Price * 1, employeeAfterUpdate$TotalSold - employee$TotalSold))
    
    # assert the expected and actual values
    test_that("Test Update Trigger:", {
      expect_equal(employeeAfterUpdate$TotalSold - employee$TotalSold, productPrice$Price * 1)
    })
    cat("\n")
  }, error = function(e) {
    cat("Error while validating update trigger: ", e$message, "\n")
  })
}

validateDeleteTrigger <- function(dbCon) {
  tryCatch({
    cat("Validating delete trigger\n")
    # sample data for validation
    employeeIdToTest <- 3
    productIdToTest <- 3
    orderIdToTest <- 10253
    
    # get the total sold of employee before deleting a row in OrderDetails
    employee <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold by the employee with the id %s is %s\n", employeeIdToTest, employee$TotalSold))
    
    # get the price of the product
    productPrice <- dbGetQuery(dbCon, sprintf("SELECT Price FROM Products where ProductID = %s;", productIdToTest))
    cat(sprintf("Price of the product for 1 unit is %s\n", productPrice$Price))
    
    # delete the row in OrderDetails
    dbExecute(dbCon, sprintf("DELETE FROM OrderDetails WHERE OrderID = %s AND ProductID = %s;", orderIdToTest, productIdToTest))
    
    # get the total sold of employee after deleting a row in OrderDetails
    employeeAfterDelete <- dbGetQuery(dbCon, sprintf("SELECT * FROM Employees where EmployeeID = %s;", employeeIdToTest))
    cat(sprintf("Total sold by the employee with the id %s after deleting quantity is %s\n", employeeIdToTest, employeeAfterDelete$TotalSold))
    
    # validate the trigger by checking if the TotalSold column in Employees table is updated
    cat(sprintf("Expected decrease in price: %s, actual:%s\n", productPrice$Price * 2, employee$TotalSold - employeeAfterDelete$TotalSold))
    
    # assert the expected and actual values
    test_that("Test Delete Trigger:", {
      expect_equal(employee$TotalSold - employeeAfterDelete$TotalSold, productPrice$Price * 2)
    })
    cat("\n")
  }, error = function(e) {
    cat("Error while validating delete trigger: ", e$message, "\n")
  })
}

validateTriggers <- function(dbCon) {
  # validate the insert trigger
  validateInsertTrigger(dbCon)
  # validate the update trigger
  validateUpdateTrigger(dbCon)
  # validate the delete trigger
  validateDeleteTrigger(dbCon)
}

# Main function
main <- function() {
  # Step 1: Load required libraries
  installPackages(c("RSQLite", "testthat"))
  
  # Step 2: Connect to the SQLite database
  dbCon <- ConnectSQLiteDB("OrdersDB.sqlitedb.db")
  
  # Step 3: Alter the Employees table and add the TotalSold column
  AddTotalSoldInEmployeesTable(dbCon)
  
  # Step 4: Update the TotalSold column with the total revenue for each employee
  updateTotalSoldColumnInEmployeesTable(dbCon)
  
  # Step 5: Create the triggers
  insertTriggerOnOrderDetailsTable(dbCon)
  updateTriggerOnOrderDetailsTable(dbCon)
  deleteTriggerOnOrderDetailsTable(dbCon)
  
  # Step 6: validate the triggers
  validateTriggers(dbCon)
  
  # Step 7: Disconnect from the database
  dbDisconnect(dbCon)
}

# Run the main function
main()
