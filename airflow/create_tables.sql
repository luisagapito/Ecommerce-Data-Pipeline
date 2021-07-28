CREATE TABLE IF NOT EXISTS public.product_sales
(
	event_time TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE RAW
	,product_id VARCHAR(256) NOT NULL  ENCODE RAW
	,product_name VARCHAR(256)   ENCODE lzo
	,product_brand VARCHAR(256)   ENCODE lzo
	,product_price DOUBLE PRECISION   ENCODE RAW
	,customer_id VARCHAR(256) NOT NULL  ENCODE lzo
	,customer_payment_method VARCHAR(256)   ENCODE lzo
	,customer_total_charges DOUBLE PRECISION   ENCODE RAW
	,customer_contract VARCHAR(256)   ENCODE lzo
	,PRIMARY KEY (customer_id, product_id, event_time)
)
DISTSTYLE KEY
 DISTKEY (customer_id)
 SORTKEY (
	product_id
	, event_time
	)
;

CREATE TABLE IF NOT EXISTS public.products
(
	product_id VARCHAR(256) NOT NULL  ENCODE RAW
	,product_name VARCHAR(256)   ENCODE lzo
	,product_type VARCHAR(256)   ENCODE lzo
	,product_description VARCHAR(65535)   ENCODE lzo
	,PRIMARY KEY (product_id)
)
DISTSTYLE KEY
 DISTKEY (product_id)
 SORTKEY (
	product_id
	)
;


CREATE TABLE IF NOT EXISTS public.staging_customer
(
	gender VARCHAR(256)   ENCODE lzo
	,seniorcitizen VARCHAR(256)   ENCODE lzo
	,partner VARCHAR(256)   ENCODE lzo
	,dependents VARCHAR(256)   ENCODE lzo
	,tenure VARCHAR(256)   ENCODE lzo
	,phoneservice VARCHAR(256)   ENCODE lzo
	,multiplelines VARCHAR(256)   ENCODE lzo
	,internetservice VARCHAR(256)   ENCODE lzo
	,onlinesecurity VARCHAR(256)   ENCODE lzo
	,onlinebackup VARCHAR(256)   ENCODE lzo
	,deviceprotection VARCHAR(256)   ENCODE lzo
	,techsupport VARCHAR(256)   ENCODE lzo
	,streamingtv VARCHAR(256)   ENCODE lzo
	,streamingmovies VARCHAR(256)   ENCODE lzo
	,contract VARCHAR(256)   ENCODE lzo
	,paperlessbilling VARCHAR(256)   ENCODE lzo
	,paymentmethod VARCHAR(256)   ENCODE lzo
	,monthlycharges VARCHAR(256)   ENCODE lzo
	,totalcharges VARCHAR(256)   ENCODE lzo
	,churn VARCHAR(256)   ENCODE lzo
	,user_id VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;


CREATE TABLE IF NOT EXISTS public.staging_events
(
	event_time VARCHAR(256)   ENCODE lzo
	,event_type VARCHAR(256)   ENCODE lzo
	,product_id VARCHAR(256)   ENCODE lzo
	,category_id VARCHAR(256)   ENCODE lzo
	,category_code VARCHAR(256)   ENCODE lzo
	,brand VARCHAR(256)   ENCODE lzo
	,price VARCHAR(256)   ENCODE lzo
	,user_id VARCHAR(256)   ENCODE lzo
	,user_session VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;

CREATE TABLE IF NOT EXISTS public.staging_products
(
	name VARCHAR(256)   ENCODE lzo
	,product_type VARCHAR(256)   ENCODE lzo
	,img VARCHAR(256)   ENCODE lzo
	,rating VARCHAR(256)   ENCODE lzo
	,dupes VARCHAR(256)   ENCODE lzo
	,description VARCHAR(65535)   ENCODE lzo
	,shade_img VARCHAR(256)   ENCODE lzo
	,price_site VARCHAR(256)   ENCODE lzo
	,view_count VARCHAR(256)   ENCODE lzo
	,product_id VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;

CREATE TABLE IF NOT EXISTS public."time"
(
	event_time TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE RAW
	,"hour" INTEGER   ENCODE az64
	,"day" INTEGER   ENCODE az64
	,week INTEGER   ENCODE az64
	,"month" INTEGER   ENCODE az64
	,"year" INTEGER   ENCODE az64
	,weekday INTEGER   ENCODE az64
	,PRIMARY KEY (event_time)
)
DISTSTYLE KEY
 DISTKEY (event_time)
 SORTKEY (
	event_time
	)
;


CREATE TABLE IF NOT EXISTS public.users
(
	customer_id VARCHAR(256) NOT NULL  ENCODE RAW
	,gender VARCHAR(256)   ENCODE lzo
	,senior_citizen INTEGER   ENCODE az64
	,partner INTEGER   ENCODE az64
	,dependents INTEGER   ENCODE az64
	,phone_service INTEGER   ENCODE az64
	,internet_service INTEGER   ENCODE az64
	,PRIMARY KEY (customer_id)
)
DISTSTYLE KEY
 DISTKEY (customer_id)
 SORTKEY (
	customer_id
	)
;