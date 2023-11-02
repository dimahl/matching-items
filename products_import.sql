BEGIN TRANSACTION

BEGIN try

	DECLARE @codePrefix varchar(10) = 'org_'
	DECLARE @msg nvarchar(1000) = ''
	DECLARE @count_1 int
	DECLARE @count_2 int

--=============================================================================
-- Create	import
--=============================================================================
EXEC VendorMigrator..LogEvent 'info', N'Create import.';

	-- 1 - products with general fields and categories,
	-- 2 - products attributes,
	-- 4 - products images,
	-- 8 - products prices,
	-- 16 - bullet points
	-- 32 - product files (resources)
	-- 64 - product stock
	-- 128 - related products
	-- 256 - product groups
	-- 512 - discontinued
	-- 1024 - removed
	-- 2048 - keyword



	--DECLARE @importContentFlags bigint = (1 | 2 | 4 | 16 | 32 | 128 | 1024);
	DECLARE @importContentFlags bigint = (1 | 2 | 4 | 16 | 1024);
	DECLARE @importId int;
	EXEC VendorMigrator..CreateVendorImport
		@vendorName = 'ORGILL',
		@description = N'Products import  ORGILL',
		@contentFlags = @importContentFlags,
		@importId = @importId output

	DECLARE @vendorId int
	SELECT @vendorId = vendor_id FROM VendorMigrator..vendor_imports WHERE id = @importId;
----------------------------------------------------------------------------------------------------
	-- Prepare products
	--DROP TABLE IF EXISTS #process_products;
	--DROP TABLE IF EXISTS #exclude_products_id;
	--DROP TABLE IF EXISTS #process_products_attributes;
	--DROP TABLE IF EXISTS #process_products_resources;
	--DROP TABLE IF EXISTS #process_products_images_temp;

	-- CREATING the products_id table to be excluded
	
	-- cut-off time only for removed products 	
	DECLARE @remove_days int = 30 -- 7 days default
	SELECT @remove_days = [days] FROM dkh_scrappers..removed_candidates_period WHERE vendor_id = @VendorId
	
	SELECT *
		INTO #exclude_products_id
	FROM (
		
			 SELECT p.item_code 
			 FROM products p 
			 WHERE p.name LIKE '%obsolete%' 
			    OR p.name LIKE '%obsol %'
			    OR p.name LIKE '%oboslete %'
			    OR p.name LIKE '%rental%'
					
			UNION 
			
			SELECT pc.product_item_code 
			FROM products_categories pc  
			WHERE pc.category_code like '70-705-70540-%'	--sporting goods >>  HUNTING
			   OR pc.category_code like '70-705-70550-%'	--sporting goods >>  GUNS & ACCESSORIES
			   OR pc.category_code like '70-705-70560-%'	--sporting goods >>  HUNTING APPAREL
			   OR pc.category_code like '70-705-70565-%'	--sporting goods >>  KNIVES & ACCESS
			   OR pc.category_code like '75-751-%'			--store & office supplies >> STORE SUPPLIES

			UNION 
			
			SELECT item_code
			FROM products 
			where item_code in(7995202, 5260815, 5486204) 
			--7995202 Promotional Item, the discount DK does not work  
			--5260815 duplicate with 6311195
			--5486204 discontinued exist actual
			
			UNION 
			
			--only for new products import, that not to import removed products with stock = 0
			SELECT p.item_code
			FROM products p
			JOIN stock s ON s.product_item_code = p.item_code AND s.stock = 0
			JOIN DKH_scrappers.dbo.removed_candidates rc ON rc.product_item_code = s.product_item_code AND rc.vendor_id = @VendorId
				AND rc.removed_date < DATEADD(day, -@remove_days, GETUTCDATE())
			
		) AS excluded_products;

	SELECT pim.*
		INTO #process_products_images_temp
	FROM products_images pim
	WHERE pim.name NOT IN ('', 'no_image.jpg') 
		AND pim.name NOT LIKE '%MISSING%.jpg' 
		AND (
				pim.file_name LIKE '%.jpg'
			OR	pim.file_name LIKE '%.jpeg'
			OR	pim.file_name LIKE '%.bmp'
			OR	pim.file_name LIKE '%.gif'
			OR	pim.file_name LIKE '%.png'
			)

	
	SELECT DISTINCT p.*
		INTO #process_products
	FROM products p
	JOIN stock s ON s.product_item_code = p.item_code AND s.warehouse_code in ('002', '004', '007')--need product ONLY this warehouse
	JOIN #process_products_images_temp t on t.product_item_code = p.item_code -- many new products, which have not yet image, later it's ok
	LEFT JOIN #exclude_products_id ep ON ep.item_code = p.item_code
	WHERE ep.item_code IS NULL AND s.stock > 0--test

	UPDATE #process_products SET manufacturer = NULL WHERE trim(manufacturer) = ''
	UPDATE #process_products SET brand = NULL WHERE trim(brand) = ''
	DELETE #process_products WHERE manufacturer IS NULL AND brand is NULL;

	UPDATE #process_products SET item_code = NULL WHERE trim(item_code) = ''
	UPDATE #process_products SET manufacturer_product_code = NULL WHERE trim(manufacturer_product_code) = ''
	DELETE #process_products WHERE item_code IS NULL AND manufacturer_product_code is NULL;

	DROP TABLE #process_products_images_temp;

--=============================================================================
-- Products categories
--=============================================================================
EXEC VendorMigrator..LogEvent 'info', N'Add products categories.';

	SELECT
		pc.product_item_code,
		pc.category_code,
		0 AS is_main
		INTO #process_products_categories
	FROM products_categories pc
	JOIN #process_products p ON pc.product_item_code = p.item_code;

	-- orgill does not have more 1 category, but fix just in case
	WITH main_category_cte AS 
      (
        SELECT MIN(category_code) AS category_code, product_item_code
        FROM #process_products_categories
        GROUP BY product_item_code
      )
  	UPDATE pc
		SET is_main=1
	FROM #process_products_categories pc
	JOIN main_category_cte c ON pc.category_code=c.category_code AND pc.product_item_code=c.product_item_code
	
	
SET @msg = (SELECT CAST(count(1) AS nvarchar(10))+ N' product categories.' FROM #process_products_categories)
EXEC VendorMigrator..LogEvent 'info', @msg

	INSERT INTO VendorMigrator..vendor_products_categories
		(
			import_id,
			product_import_code,
			category_code,
			is_main
		)
		SELECT
			@importId,
			@codePrefix + pc.product_item_code,
			pc.category_code,
			COALESCE(pc.is_main, 0) as is_main
		FROM #process_products_categories pc

SET @msg = CAST(@@ROWCOUNT AS nvarchar(10)) + N' product categories added.'
EXEC VendorMigrator..LogEvent 'info', @msg

--=============================================================================
-- Categories
--=============================================================================
EXEC VendorMigrator..LogEvent 'info', N'Add categories.';

	INSERT INTO VendorMigrator..vendor_categories
		(
			code,
			vendor_id,
			parent_code,
			name,
			import_id
		)
		SELECT DISTINCT
			c.code,
			@vendorId AS vendor_id,
			c.parent_code,
			c.name,
			@importId AS import_id
		FROM categories c
	    JOIN #process_products_categories pc ON pc.category_code = c.code
		
	    
SET @msg = CAST(@@ROWCOUNT AS nvarchar(10)) + N' categories added.'
EXEC VendorMigrator..LogEvent 'info', @msg

DROP TABLE #process_products_categories

--=============================================================================
-- Products
--=============================================================================
EXEC VendorMigrator..LogEvent 'info', N'Add products.';

	DECLARE @spec_symbol nvarchar(100) = '" -_^$@#,' -- symbol
	
	SELECT pa.*
		INTO #process_products_attributes
	FROM products_attributes pa
	JOIN #process_products pp ON pa.product_item_code = pp.item_code
	WHERE (pa.name NOT LIKE 'Systen..%' AND pa.name NOT LIKE 'System..%')
		OR pa.name IN ('System..PackLength', 'System..PackWidth', 'System..PackHeight', 
						'System..Quantity Round Option', 'System..Cubic Divisor', 
						'System..Factory Pack Size', 'System..Ship By UPS FedEx USPS')

	-- select columns with long>120 value
	SELECT DISTINCT pa.name
		INTO #columns_to_desc
	FROM #process_products_attributes pa 
	WHERE len(pa.value) > 120;	
	
	-- create aggregation string 
	SELECT s.product_item_code, '<br><b>Specifications:</b><br>'+STRING_AGG(s.spec,'') spec
		INTO #spec
	FROM(
		SELECT pa.*
		    ,CASE WHEN CHARINDEX(';', pa.value)>0
		      THEN '<p>'+pa.name+':</p><ul><li>'+REPLACE(pa.value,';','</li><li>')+'</li></ul>'
		      ELSE '<p>'+pa.name+': '+ pa.value+'</p>' END spec
		FROM #process_products_attributes pa
		WHERE pa.name IN (SELECT * FROM #columns_to_desc) AND trim(@spec_symbol FROM pa.value) != ''
	    ) s
	GROUP BY s.product_item_code

	UPDATE #process_products
		SET description = NULL
	WHERE trim(@spec_symbol FROM description) = ''
	
	UPDATE #process_products
		SET name = trim(description)
	WHERE name IS NULL
	
	SELECT pa.product_item_code, trim(@spec_symbol FROM pa.value) AS value  
		INTO #long_desc
	FROM products_attributes pa
	JOIN #process_products p on pa.product_item_code = p.item_code
	WHERE pa.name = 'System..Long Descriptions' AND trim(@spec_symbol FROM pa.value) != ''
	
	UPDATE #process_products
		SET manufacturer_product_code = NULL
	WHERE manufacturer_product_code IS NULL AND trim(@spec_symbol FROM manufacturer_product_code) != ''
	
	-- restore normal name like vendor
	UPDATE #process_products SET name = concat(brand, ' ', manufacturer_product_code, name) WHERE name LIKE '/%'
	UPDATE #process_products SET name = concat(brand, ' ', manufacturer_product_code) WHERE name IS NULL AND trim(name) = ''

	--delete from begin title substring like #99999 
 	UPDATE #process_products SET name = right(name, len(name)-3) WHERE name LIKE '#[0-9] %'
 	UPDATE #process_products SET name = right(name, len(name)-4) WHERE name LIKE '#[0-9][0-9] %'
	UPDATE #process_products SET name = right(name, len(name)-4) WHERE name LIKE '#[0-9][0-9][0-9] %'
	UPDATE #process_products SET name = right(name, len(name)-5) WHERE name LIKE '#[0-9][0-9][0-9][0-9] %'
	UPDATE #process_products SET name = right(name, len(name)-6) WHERE name LIKE '#[0-9][0-9][0-9][0-9][0-9] %'
		
	UPDATE #process_products SET weight = 0	WHERE weight IS NULL
	
	UPDATE #process_products SET min_order = 1 WHERE min_order = 0 OR min_order IS NULL
	
	UPDATE #process_products SET freight_only = 1 WHERE weight >= 100
	UPDATE #process_products SET upc = NULL WHERE upc LIKE '%00000000000%' OR upc LIKE '%99999999999'
	--11zero --11 "9"
	
	UPDATE p 
		SET p.name = concat(trim(p.name), ' - ', pa.value, ' pairs')
    FROM #process_products p 
    JOIN products_attributes pa ON pa.product_item_code = p.item_code AND pa.name = 'System..UOMSize'
    WHERE p.unit_measure = 'PR'
    
	UPDATE p 
		SET p.name = concat(trim(p.name),' - ', pa.value)
    FROM #process_products p 
    JOIN products_attributes pa ON pa.product_item_code = p.item_code AND pa.name = 'System..UOMSize'
    WHERE p.unit_measure != 'PR'
    
    
        
-- 	 oversize by Shipping Unit Dimensions
    ;WITH cte_oversize AS
     	(
	     SELECT product_item_code
	     FROM products_attributes
	     WHERE name IN ('System..PackLength', 'System..PackHeight', 'System..PackWidth')
	     GROUP BY product_item_code
	     HAVING max(CAST(CAST(trim(' "' FROM value) as float) AS int)) > 48   
     	)
	update #process_products set oversize = 1 where item_code in (select product_item_code from cte_oversize)

--	 freigth only by Shipping Unit Dimensions
    ;WITH cte_freight_only AS
    	(
	     SELECT product_item_code
	     FROM products_attributes
	     WHERE name IN ('System..PackLength', 'System..PackHeight', 'System..PackWidth')
	     GROUP BY product_item_code
	     HAVING max(CAST(CAST(trim(' "' FROM value) as float) AS int)) > 96   
    	)
	update #process_products set freight_only = 1 where item_code in (select product_item_code from cte_freight_only)
    
--	UPDATE #process_products 
--		SET freight_only = 1
--	FROM #process_products pp
--	JOIN #process_products_attributes ppa ON ppa.product_item_code = pp.item_code AND ppa.name = 'System..Ship By UPS FedEx USPS'
--	WHERE ppa.value = 'N'
	 
	UPDATE #process_products set hazmat = NULL WHERE hazmat = 0


	INSERT INTO VendorMigrator..vendor_products
		(
		import_id,
		product_import_code,
		name,
		description,	
		item_code,
		manufacturer_product_code,
		weight,
		upc_code,
		min_order,
		color,
		oversize,
		must_cut,
		freight_only,
		retail_price,
		unit_measure,
		manufacturer,
		brand,
		pack_size
		)
	SELECT 
		@importId AS import_id,
		@codePrefix + trim(pp.item_code) AS product_import_code, 
		trim(pp.name) AS name,
		COALESCE(pp.description,'') + iif(s.product_item_code IS NOT NULL, s.spec,'') + iif(ld.product_item_code IS NOT NULL, '<p>'+ld.value+'</p>','') AS description,
		trim(pp.item_code) AS item_code,
		COALESCE(pp.manufacturer_product_code, trim(pp.item_code)) AS manufacturer_product_code,
		pp.weight,
		iif(pp.upc IS NOT NULL AND trim(pp.upc) != '', trim(pp.upc), NULL) AS upc_code,
		min_order,
		pp.color,
		pp.oversize,
		pp.must_cut,
		pp.freight_only,
		pp.retail_price,
		iif(pp.unit_measure IS NOT NULL AND trim(pp.unit_measure) != '', trim(pp.unit_measure), 'EA') AS unit_measure,
		COALESCE(pp.manufacturer, pp.brand) AS manufacturer,
		COALESCE(pp.brand, pp.manufacturer) AS brand,
		COALESCE(paps.value, 1) AS pack_size
	FROM #process_products pp
	LEFT JOIN #long_desc ld ON ld.product_item_code = pp.item_code 
	LEFT JOIN products_attributes paps ON paps.product_item_code = pp.item_code AND paps.name = 'System..Pack Size'
	LEFT JOIN #spec s ON s.product_item_code = pp.item_code
	
	
SET @msg = CAST(@@ROWCOUNT AS nvarchar(10)) + N' products added.'
EXEC VendorMigrator..LogEvent 'info', @msg

DROP TABLE #exclude_products_id
DROP TABLE #long_desc
DROP TABLE #spec

--=============================================================================
-- Products attributes
--=============================================================================
EXEC VendorMigrator..LogEvent 'info', N'Add products attributes.';

	UPDATE #process_products_attributes SET name = 'Shipping Dimensions Lenght' WHERE name = 'System..PackLength'
	UPDATE #process_products_attributes SET name = 'Shipping Dimensions Width' WHERE name = 'System..PackWidth'
	UPDATE #process_products_attributes SET name = 'Shipping Dimensions Height' WHERE name = 'System..PackHeight'
	UPDATE #process_products_attributes SET name = 'Break Ctn. Code' WHERE name = 'System..Quantity Round Option'
	UPDATE #process_products_attributes SET name = 'Shelf Pack' WHERE name = 'System..Cubic Divisor'
	UPDATE #process_products_attributes SET name = 'Factory Pack Size' WHERE name =	'System..Factory Pack Size'
	UPDATE #process_products_attributes SET name = 'Shipping By UPS FedEx USPS' WHERE name = 'System..Ship By UPS FedEx USPS'


	INSERT INTO VendorMigrator..vendor_products_attributes
		(
			import_id,
			product_import_code,
			name,
			value
		)
		SELECT 
			@importId,
			@codePrefix + ppa.product_item_code,
			ppa.name,
			ppa.value
		FROM #process_products_attributes ppa
		WHERE ppa.name NOT IN (SELECT * FROM #columns_to_desc)
		
		SET @count_1 = @@ROWCOUNT	

SET @msg = CAST(@count_1 AS nvarchar(10)) + N' product attributes added.'
EXEC VendorMigrator..LogEvent 'info', @msg

EXEC VendorMigrator..LogEvent 'info', N'Add ''empty'' attributes.';	
	
	INSERT INTO VendorMigrator..vendor_products_attributes
		(
			import_id,
			product_import_code,
			name,
			value
		)
		SELECT
			@importId,
			@codePrefix + item_code,
			'$$$empty$$$' as name,
			'$$$empty$$$' as value
		FROM (
				SELECT item_code  
				FROM #process_products
				
				EXCEPT
				
				SELECT product_item_code 
				FROM #process_products_attributes
				WHERE name NOT IN (SELECT * FROM #columns_to_desc)
			 ) AS non_transferred_products_id
		
		SET @count_2 = @@ROWCOUNT
		
SET @msg = CAST(@count_2 AS nvarchar(10)) + N' product ''empty'' attributes added.'
EXEC VendorMigrator..LogEvent 'info', @msg		

SET @msg = CAST(@count_1 + @count_2 AS nvarchar(10)) + N' total product attributes added.'
EXEC VendorMigrator..LogEvent 'info', @msg

DROP TABLE #columns_to_desc

--=============================================================================
-- Products bulletpoints
--=============================================================================
EXEC VendorMigrator..LogEvent 'info', N'Add products bulletpoints.';

	SELECT *
		INTO #process_products_bullet_points
	FROM products_bullet_points pbp
	JOIN #process_products pp on pbp.product_item_code = pp.item_code
	WHERE pbp.value NOT LIKE '%SKU%[0-9]%'

	INSERT INTO VendorMigrator..vendor_products_bullet_points
		(
			import_id,
			product_import_code,
			value,
			order_index
		)
		SELECT
			@importId,
			@codePrefix + pbp.product_item_code,
			trim('-*, ' FROM pbp.value) AS value,
			pbp.order_index
		FROM #process_products_bullet_points pbp
	
	SET @count_1 = @@ROWCOUNT
			
SET @msg = CAST(@count_1 AS nvarchar(10)) + N' product bulletpoints added.'
EXEC VendorMigrator..LogEvent 'info', @msg

EXEC VendorMigrator..LogEvent 'info', N'Add ''empty'' products bulletpoints.';	

	INSERT INTO VendorMigrator..vendor_products_bullet_points
		(
			import_id,
			product_import_code,
			value,
			order_index
		)
		SELECT
			@importId,
			@codePrefix + item_code,
			'$$$empty$$$',
			1
		FROM (
				SELECT item_code
				FROM #process_products
				
				EXCEPT 
				
				SELECT product_item_code
				FROM #process_products_bullet_points
			 ) AS non_transferred_products_id
	 
	SET @count_2 = @@ROWCOUNT
		
SET @msg = CAST(@count_2 AS nvarchar(10)) + N' product ''empty'' bulletpoints added.'
EXEC VendorMigrator..LogEvent 'info', @msg	


SET @msg = CAST(@count_1 + @count_2 AS nvarchar(10)) + N' total product bulletpoints added.'
EXEC VendorMigrator..LogEvent 'info', @msg

DROP TABLE #process_products_bullet_points

--=============================================================================
-- Products images
--=============================================================================
EXEC VendorMigrator..LogEvent 'info', N'Add products images.';

-- orgill does not have more "bad images", but fix just in case
	SELECT pim.*
		INTO #process_products_images
	FROM products_images pim
	JOIN #process_products p ON p.item_code = pim.product_item_code 
	WHERE pim.name NOT IN ('', 'no_image.jpg') 
				AND pim.name NOT LIKE '%MISSING%.jpg' 
				AND (
						pim.file_name LIKE '%.jpg'
					OR	pim.file_name LIKE '%.jpeg'
					OR	pim.file_name LIKE '%.bmp'
					OR	pim.file_name LIKE '%.gif'
					OR	pim.file_name LIKE '%.png'
					)

	
	INSERT INTO VendorMigrator..vendor_products_images
		(
			import_id,
			product_import_code,
			name,
			file_name,
			file_hash,
			is_main,
			storage_type
		)
		SELECT
			@importId,
			@codePrefix + product_item_code,
			name,
			file_name,
			file_hash,
			is_main,
			storage_type
		FROM #process_products_images
	SET @count_1 = @@ROWCOUNT
			
SET @msg = CAST(@count_1 AS nvarchar(10)) + N' product images added.'
EXEC VendorMigrator..LogEvent 'info', @msg

EXEC VendorMigrator..LogEvent 'info', N'Add ''empty'' products images.';	

	INSERT INTO VendorMigrator..vendor_products_images
		(
			import_id,
			product_import_code,
			name,
			file_name,
			is_main,
			storage_type
		)
		
		SELECT
			@importId,
			@codePrefix + item_code,
			'$$$empty$$$',
			'$$$empty$$$',
			1,
			0
		FROM (
				SELECT item_code
				FROM #process_products

				EXCEPT

				SELECT product_item_code
				FROM #process_products_images

			 ) AS non_transferred_products_id
	SET @count_2 = @@ROWCOUNT
	
SET @msg = CAST(@count_2 AS nvarchar(10)) + N' product ''empty'' images added.'
EXEC VendorMigrator..LogEvent 'info', @msg	


SET @msg = CAST(@count_1 + @count_2 AS nvarchar(10)) + N' total product images added.'
EXEC VendorMigrator..LogEvent 'info', @msg

DROP TABLE #process_products_images

--=============================================================================
-- Products resources
--=============================================================================
--EXEC VendorMigrator..LogEvent 'info', N'Add products resources.';	
--	
--	SELECT pr.* 
--		INTO #process_products_resources
--	FROM products_resources pr
--	JOIN #process_products pp ON pr.product_item_code = pp.item_code
--	WHERE pr.file_hash IS NOT NULL AND pr.file_hash != '' AND pr.name NOT LIKE '%price%' AND pr.name NOT LIKE '%MarketingLabel%';
--	--%MarketingLabel% -- files are damaged
--
--
--	UPDATE #process_products_resources
--		SET resource_type='catalog'
--	WHERE name LIKE '%catalog%' OR name LIKE '%brochure%' OR name LIKE '%Broucher%' OR name LIKE '%Brouchure%'
--	
--	UPDATE #process_products_resources
--		SET resource_type='msds'
--	WHERE name LIKE '%MSDS%'
--	
--	UPDATE #process_products_resources
--		SET resource_type='techDoc'
--	WHERE name LIKE '%Instruction%' OR name LIKE '%Installation%' OR name LIKE '%Manual%' OR name LIKE '%Guide%'  OR name LIKE '%Parts%List%' 
--		OR name LIKE '%data%sheet%' OR name LIKE '%spec%' OR name LIKE '%Troubleshooting Guide%' OR name LIKE '%Instrucion%' --OR name LIKE 'Submittal_Sheet'
--		OR name LIKE 'ProSource_[0-9]%[0-9].pdf'
--	
--	UPDATE #process_products_resources
--		SET resource_type='spec'
--	WHERE name LIKE '%Warning%' OR name LIKE '%_SDS%' OR name LIKE '%Warranty%' OR name LIKE '%Return%Form%' AND name LIKE '%prop%65%'
--	
--	UPDATE #process_products_resources
--		SET resource_type='imgSpec'
--	WHERE name LIKE '%Drawing%' OR name LIKE '%Dimension%'
--
--	UPDATE #process_products_resources 	
--		SET resource_type='spec'
--	WHERE resource_type='undefined' 
--
--
--	INSERT INTO VendorMigrator..vendor_products_resources
--		(
--			import_id,
--			product_import_code,
--			name,
--			description,
--			resource_file,
--			resource_hash,
--			resource_type,
--			storage_type
--		)
--		SELECT
--			@importId,
--			@codePrefix + pr.product_item_code,
--			pr.name,
--			iif(trim(pr.description)='', NULL, pr.description),
--			pr.file_name,
--			pr.file_hash,
--			pr.resource_type,
--			pr.storage_type
--		FROM #process_products_resources pr
--	SET @count_1 = @@ROWCOUNT
--			
--SET @msg = CAST(@count_1 AS nvarchar(10)) + N' product resources added.'
--EXEC VendorMigrator..LogEvent 'info', @msg
--
--EXEC VendorMigrator..LogEvent 'info', N'Add ''empty'' products resources.';	
--	
--	INSERT INTO VendorMigrator..vendor_products_resources
--		(
--			import_id,
--			product_import_code,
--			name,
--			resource_file,
--			resource_hash,
--			resource_type,
--			storage_type
--		)
--
--		SELECT
--			@importId,
--			@codePrefix + item_code,
--			'$$$empty$$$',
--			'$$$empty$$$',
--			'$$$empty$$$',
--			'$$$empty$$$',
--			0
--		FROM (
--				SELECT item_code
--				FROM #process_products
--				
--				EXCEPT 
--				
--				SELECT product_item_code
--				FROM #process_products_resources
--			 
--			 ) AS non_transferred_products_id
--	SET @count_2 = @@ROWCOUNT
--	
--SET @msg = CAST(@count_2 AS nvarchar(10)) + N' product ''empty'' resources added.'
--EXEC VendorMigrator..LogEvent 'info', @msg	
--
--SET @msg = CAST(@count_1 + @count_2 AS nvarchar(10)) + N' total product resources added.'
--EXEC VendorMigrator..LogEvent 'info', @msg
--
--DROP TABLE #process_products_resources

--=============================================================================
-- Related products
--=============================================================================
--EXEC VendorMigrator..LogEvent 'info', N'Add related products.';	
--	
--	INSERT INTO VendorMigrator..vendor_products_related 
--		(
--		import_id, 
--		product_import_code, 
--		related_product_import_code
--		)
--		
--		SELECT 
--			@importId, 
--			@codePrefix + rp.product_item_code, 
--			@codePrefix + rp.related_product_code
--		FROM related_products rp
--		INNER JOIN #process_products pp on rp.product_item_code = pp.item_code
--		INNER JOIN #process_products pp1 on rp.related_product_code = pp1.item_code
--		WHERE product_item_code != related_product_code
--	SET @count_1 = @@ROWCOUNT
--			
--SET @msg = CAST(@count_1 AS nvarchar(10)) + N' related products added.'
--EXEC VendorMigrator..LogEvent 'info', @msg
--	
--EXEC VendorMigrator..LogEvent 'info', N'Add ''empty'' related products.';	
--
--	INSERT INTO VendorMigrator..vendor_products_related
--		(
--		import_id,
--		product_import_code,
--		related_product_import_code
--		)
--		
--		SELECT
--			@importId,
--			@codePrefix + item_code,
--			'$$$empty$$$'
--		FROM (
--				SELECT item_code
--				FROM #process_products
--				
--				EXCEPT 
--				
--				SELECT rp.product_item_code
--				FROM related_products rp
--				INNER JOIN #process_products pp ON rp.product_item_code = pp.item_code
--				INNER JOIN #process_products pp1 on rp.related_product_code = pp1.item_code
--				WHERE product_item_code != related_product_code
--			 ) AS non_transferred_products_id
--	SET @count_2 = @@ROWCOUNT
--	
--SET @msg = CAST(@count_2 AS nvarchar(10)) + N' product ''empty'' related added.'
--EXEC VendorMigrator..LogEvent 'info', @msg	
--
--SET @msg = CAST(@count_1 + @count_2 AS nvarchar(10)) + N' total related products added.'
--EXEC VendorMigrator..LogEvent 'info', @msg	
--	
--=============================================================================
-- Products meta
--=============================================================================
EXEC VendorMigrator..LogEvent 'info', N'Add products meta.';	
 
	--upc
	INSERT INTO VendorMigrator..vendor_products_meta
		(
			import_id,
			product_import_code,
			code,
			value
		)
	SELECT DISTINCT
		@importId,
		@codePrefix + item_code,
		'UPC' AS code,
		upc
	FROM  #process_products 
	WHERE upc IS NOT NULL AND trim(upc) != ''
	
    --mpn
	INSERT INTO VendorMigrator..vendor_products_meta
		(
			import_id,
			product_import_code,
			code,
			value
		)
	SELECT DISTINCT
		@importId,
		@codePrefix + item_code,
		'mpn' as code,
		COALESCE(manufacturer_product_code, item_code) AS value
	FROM #process_products 
  
    --itemcode
	INSERT INTO VendorMigrator..vendor_products_meta
		(
			import_id,
			product_import_code,
			code,
			value
		)
	SELECT DISTINCT
		@importId,
		@codePrefix + item_code,
		'itemCode' AS code,
		item_code
	FROM #process_products

	--removed
	INSERT INTO VendorMigrator..vendor_products_meta
		(
			import_id,
			product_import_code,
			code,
			value
		)
	SELECT DISTINCT
		@importId,
		@codePrefix + item_code,
		'removed' as code,
		0 as value
	FROM #process_products
	
	--retail price
	INSERT INTO VendorMigrator..vendor_products_meta
  		(
  			import_id,
  			product_import_code,
  			code,
  			value
  		)
  	SELECT DISTINCT
  		@importId,
  		@codePrefix + item_code,
  		'system..retail_price' AS code,
  		retail_price AS value
  	FROM #process_products
  	WHERE retail_price IS NOT NULL
  	
  	--replacement
	INSERT INTO VendorMigrator..vendor_products_meta
  		(
  			import_id,
  			product_import_code,
  			code,
  			value
  		)
  	SELECT DISTINCT
  		@importId,
  		@codePrefix + pr.product_item_code,
  		'system..replacement' AS code,
  		@codePrefix + pr.replacement_item_code AS value
  	FROM products_replacements pr
  	JOIN #process_products p ON p.item_code = pr.product_item_code

	--Removed
	INSERT INTO VendorMigrator..vendor_products_removed
		(
		import_id,
		product_import_code,
		seller_id,
		removed,
		updated_date
		)
	SELECT DISTINCT
		@importId,
		@codePrefix + item_code,
		@vendorId,
		0,
		getutcdate()
	FROM #process_products

SET @msg = CAST(@@ROWCOUNT AS nvarchar(10)) + N' Not removed products added.'
EXEC VendorMigrator..LogEvent 'info', @msg
EXEC VendorMigrator..LogEvent 'info', N'Products meta added.';	

DROP TABLE #process_products_attributes
DROP TABLE #process_products
SELECT @importId;

	IF @@trancount > 0 COMMIT TRANSACTION 
END try
BEGIN CATCH  
	IF @@trancount > 0 ROLLBACK TRANSACTION;
	throw 
END catch
