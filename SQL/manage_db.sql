IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = 'HotSale')
BEGIN
	CREATE DATABASE HotSale
END

USE HotSale;

--DROP TABLE PriceHistory;
--DROP TABLE Products;
--DROP TABLE Brands;
--DROP TABLE SubCategories;
--DROP TABLE Categories;
--DROP TABLE MainCategories;

IF NOT EXISTS (
	SELECT * FROM INFORMATION_SCHEMA.TABLES 
	WHERE TABLE_NAME = 'Brands'
)
BEGIN
	CREATE TABLE Brands (
		id INT NOT NULL PRIMARY KEY IDENTITY,
		name NVARCHAR(200) NOT NULL UNIQUE,
		created_at DATETIME2 DEFAULT GETDATE()
	)
END

IF NOT EXISTS (
	SELECT * FROM INFORMATION_SCHEMA.TABLES 
	WHERE TABLE_NAME = 'MainCategories'
)
BEGIN
	CREATE TABLE MainCategories (
		id INT NOT NULL PRIMARY KEY IDENTITY,
		name NVARCHAR(200) NOT NULL UNIQUE,
		created_at DATETIME2 DEFAULT GETDATE()
	)
END

IF NOT EXISTS (
	SELECT * FROM INFORMATION_SCHEMA.TABLES 
	WHERE TABLE_NAME = 'Categories'
)
BEGIN
	CREATE TABLE Categories (
		id INT NOT NULL PRIMARY KEY IDENTITY,
		name NVARCHAR(200) NOT NULL UNIQUE,
		main_category_id INT NOT NULL,
		created_at DATETIME2 DEFAULT GETDATE(),
		CONSTRAINT FK_Categories_MainCategories FOREIGN KEY (main_category_id) REFERENCES MainCategories(id)
	)
END

IF NOT EXISTS (
	SELECT * FROM INFORMATION_SCHEMA.TABLES 
	WHERE TABLE_NAME = 'SubCategories'
)
BEGIN
	CREATE TABLE SubCategories (
		id INT NOT NULL PRIMARY KEY IDENTITY,
		name NVARCHAR(200) NOT NULL UNIQUE,
		category_id INT NOT NULL,
		created_at DATETIME2 DEFAULT GETDATE(),
		CONSTRAINT FK_SubCategories_Categories FOREIGN KEY (category_id) REFERENCES Categories(id)
	)
END

IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'Products'
)
BEGIN
    CREATE TABLE Products (
        id INT NOT NULL PRIMARY KEY IDENTITY,
        title NVARCHAR(500) NOT NULL,
        url NVARCHAR(750) NOT NULL UNIQUE,
        condition NVARCHAR(50),
        brand_id INT NOT NULL,
        main_category_id INT NOT NULL,
        warranty NVARCHAR(200),
        payment_method NVARCHAR(200),
        seller NVARCHAR(200),
        delivery_time NVARCHAR(100),
        delivery_cost NVARCHAR(100),
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE(),
        CONSTRAINT FK_Products_Brands FOREIGN KEY (brand_id) REFERENCES Brands(id),
        CONSTRAINT FK_Products_MainCategories FOREIGN KEY (main_category_id) REFERENCES MainCategories(id),
    );
END

IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'PriceHistory'
)
BEGIN
    CREATE TABLE PriceHistory (
        id INT NOT NULL PRIMARY KEY IDENTITY,
        product_id INT NOT NULL,
        extracted_at DATETIME2 NOT NULL,
        original_price DECIMAL(20,2),
        price_with_discount DECIMAL(18,2),
        discount_aplicated NVARCHAR(50),
        stock NVARCHAR(100),
        total_solds NVARCHAR(100),
        recommendation NVARCHAR(100),
        rating DECIMAL(3,2),
        total_califications NVARCHAR(100),
        quality_price_relation NVARCHAR(100),
        created_at DATETIME2 DEFAULT GETDATE(),
        CONSTRAINT FK_PriceHistory_Products FOREIGN KEY (product_id) REFERENCES Products(id)
    );
    CREATE INDEX idx_pricehistory_productid_extractedat ON PriceHistory(product_id, extracted_at DESC);
END