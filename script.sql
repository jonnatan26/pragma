-- BASE DE DATOS
-- postgres


-- public.prices definition 
-- Drop table
-- DROP TABLE public.prices;
CREATE TABLE public.prices (
	user_id int4 NOT NULL,
	price float4 NULL,
	fecha date NOT NULL,
	fecha_ingesta varchar NOT NULL
);

-- public.seguimiento definition
-- Drop table
-- DROP TABLE public.seguimiento;
CREATE TABLE public.seguimiento (
	id_archivo varchar NOT NULL,
	recuento int4 NOT NULL,
	media numeric NOT NULL,
	minima numeric NOT NULL,
	maxima numeric NOT NULL,
	descripcion varchar NOT NULL
);

-- public.monitoreo_ingestas definition
-- Drop table
-- DROP TABLE public.monitoreo_ingestas;
CREATE TABLE public.monitoreo_ingestas (
	id varchar NOT NULL,
	ruta_archivo varchar NOT NULL,
	fecha_modificacion_fuente varchar NOT NULL,
	estado varchar NOT NULL,
	fecha_ingesta varchar NOT NULL,
	error varchar NOT NULL
);