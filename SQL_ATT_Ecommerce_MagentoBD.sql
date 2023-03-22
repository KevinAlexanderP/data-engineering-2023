-- Selecciona todo de la bd basandose en los id incrementales conocidos como ECO
select count(increment_id) from full_bd_unificado2022 fbu;
-- Selecciona todos de la bd con un id incremental unico (no repetidos)
select count(distinct increment_id) from full_bd_unificado2022 fbu;

-- Selecciona fecha de pago, el conteo y agrupa los valores de la bd por el tipo que se asigne
select fecha_de_pago,tipo ,count(*)
from [dbo].[full_bd_unificado2022]
where fecha_de_pago like '%2023%' and tipo = "Renovacion"
group by fecha_de_pago,tipo
order by fecha_de_pago


select   count(*)
from full_bd_unificado2022 fbu 
where fecha_de_pago like '%2023%' and tipo = 'Pospago' and allow_esim ='si'


select fecha_de_pago,tipo,oferta, allow_esim  ,count(*)
from full_bd_unificado2022 fbu 
where fecha_de_pago like '%2023-01-2%' and tipo = 'Prepago' 
group by fecha_de_pago,tipo,oferta,allow_esim
order by fecha_de_pago

select fecha_de_pago,tipo,oferta, allow_esim  ,count(*)
from full_bd_unificado2022 fbu 
where fecha_de_pago like '%2023-%' and tipo = 'RenovaciÃ³n' 
group by fecha_de_pago,tipo,oferta,allow_esim
order by fecha_de_pago

select count(DISTINCT increment_id) from all_orders_cls_tracking_st aocts 

delete from full_bd_unificado2022  where fecha_de_pago  like '2023%';