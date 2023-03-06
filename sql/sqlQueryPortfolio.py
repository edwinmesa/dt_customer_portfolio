sqlCustomer = f"""
    SELECT     
           T350.F350_ROWID                                                      AS ROWID_DOC,
           T350.F350_ID_CIA                                                     AS COD_CIA,
           T350.F350_FECHA                                                      AS FECHA_DOC,
           T461.F461_ROWID_TERCERO_FACT                                         AS ROWID_TERCERO,
           T200.F200_NIT                                                        AS NIT,
           T200.F200_RAZON_SOCIAL                                               AS RAZON_SOCIAL,
           T461.F461_ID_SUCURSAL_FACT                                           AS COD_SUCURSAL,
           T350.F350_ID_CO                                                      AS COD_CO,
           T470.F470_ID_CO_MOVTO                                                AS COD_CO_MVTO,
           T350.F350_ID_TIPO_DOCTO                                              AS COD_DOC,
           T350.F350_CONSEC_DOCTO                                               AS CONSEC_DOC,
           T106_005.F106_DESCRIPCION                                            AS LINEA_PREVEEDOR,
           T106_035.F106_DESCRIPCION                                            AS GRUPO_SHOPPER,
           T461.F461_IND_NAT                                                    AS IND_NATURALEZA,
           SA.DEBITOS                                                           AS DEBITOS_SA,
           SA.CREDITOS                                                          AS CREDITOS_SA,
           SA.DEBITOS-SA.CREDITOS                                               AS SALDO,
           SUM((CASE
                 WHEN T470.F470_IND_NATURALEZA = 1 THEN
                  T470.F470_VLR_BRUTO * (-1)
                 ELSE
                  T470.F470_VLR_BRUTO
               END) - (CASE
                 WHEN T470.F470_IND_NATURALEZA = 1 THEN
                  T470.F470_VLR_DSCTO_LINEA * (-1)
                 ELSE
                  T470.F470_VLR_DSCTO_LINEA
               END) + (CASE
                 WHEN T470.F470_IND_NATURALEZA = 1 THEN
                  T470.F470_VLR_IMP * (-1)
                 ELSE
                  T470.F470_VLR_IMP
               END))                                                            AS VLR_FACT
      FROM T350_CO_DOCTO_CONTABLE T350
     INNER JOIN T461_CM_DOCTO_FACTURA_VENTA T461
        ON T461.F461_ROWID_DOCTO = T350.F350_ROWID
     INNER JOIN T460_CM_DOCTO_REMISION_VENTA T460
        ON T350.F350_ROWID = T460.F460_ROWID_DOCTO_FACTURA
     INNER JOIN T470_CM_MOVTO_INVENT T470
        ON T470.F470_ROWID_DOCTO = T460.F460_ROWID_DOCTO
     LEFT OUTER JOIN T460_CM_DOCTO_REMISION_VENTA T460
        ON T470.F470_ROWID_DOCTO = T460.F460_ROWID_DOCTO
       AND T461.F461_ROWID_DOCTO = T460.F460_ROWID_DOCTO_FACTURA   
      LEFT OUTER JOIN T472_CM_MOVTO_INVENT_IMP T472
        ON T472.F472_ROWID_MOVTO_INV = T470.F470_ROWID
       AND T472.F472_ID_LLAVE_IMPUESTO = 'ICO'
     INNER JOIN (SELECT 
               T353.F353_ROWID_DOCTO       AS ROWID_DCTO_SA,
              (NVL(T353.F353_TOTAL_DB, 0)) AS DEBITOS,
              (NVL(T353.F353_TOTAL_CR, 0)) AS CREDITOS
     FROM  T353_CO_SALDO_ABIERTO T353
     WHERE (NVL(T353.F353_TOTAL_DB, 0) - NVL(T353.F353_TOTAL_CR, 0)) <> 0) SA
     ON SA.ROWID_DCTO_SA = T350.F350_ROWID 
     
     LEFT JOIN T200_MM_TERCEROS T200
      ON T200.F200_ROWID = T461.F461_ROWID_TERCERO_FACT 
     
     INNER JOIN T121_MC_ITEMS_EXTENSIONES T121
        ON T121.F121_ROWID = T470.F470_ROWID_ITEM_EXT
     INNER JOIN T120_MC_ITEMS T120
        ON T120.F120_ROWID = T121.F121_ROWID_ITEM
      LEFT OUTER JOIN T125_MC_ITEMS_CRITERIOS T125_005
        ON T125_005.F125_ID_CIA = T120.F120_ID_CIA
       AND T125_005.F125_ROWID_ITEM = T120.F120_ROWID
       AND T125_005.F125_ID_PLAN = '005'
      LEFT OUTER JOIN T106_MC_CRITERIOS_ITEM_MAYORES T106_005
        ON T106_005.F106_ID_CIA = T125_005.F125_ID_CIA
       AND T106_005.F106_ID = T125_005.F125_ID_CRITERIO_MAYOR
       AND T106_005.F106_ID_PLAN = T125_005.F125_ID_PLAN
      LEFT OUTER JOIN T105_MC_CRITERIOS_ITEM_PLANES T105_005
        ON T105_005.F105_ID_CIA = T106_005.F106_ID_CIA
       AND T105_005.F105_ID = T106_005.F106_ID_PLAN
      LEFT OUTER JOIN T125_MC_ITEMS_CRITERIOS T125_035
        ON T125_035.F125_ID_CIA = T120.F120_ID_CIA
       AND T125_035.F125_ROWID_ITEM = T120.F120_ROWID
       AND T125_035.F125_ID_PLAN = '035'
      LEFT OUTER JOIN T106_MC_CRITERIOS_ITEM_MAYORES T106_035
        ON T106_035.F106_ID_CIA = T125_035.F125_ID_CIA
       AND T106_035.F106_ID = T125_035.F125_ID_CRITERIO_MAYOR
       AND T106_035.F106_ID_PLAN = T125_035.F125_ID_PLAN
      LEFT OUTER JOIN T105_MC_CRITERIOS_ITEM_PLANES T105_035
        ON T105_035.F105_ID_CIA = T106_035.F106_ID_CIA
       AND T105_035.F105_ID = T106_035.F106_ID_PLAN
     INNER JOIN T285_CO_CENTRO_OP T285
        ON T350.F350_ID_CIA = T285.F285_ID_CIA
       AND T350.F350_ID_CO = T285.F285_ID
      LEFT OUTER JOIN T430_CM_PV_DOCTO T430
        ON T460.F460_ROWID_PV_DOCTO = T430.F430_ROWID
      LEFT OUTER JOIN T160_MC_CLI_CONTADO T160
        ON T160.F160_ROWID = T461.F461_ROWID_CLI_CONTADO
     INNER JOIN T146_MC_MOTIVOS T146
        ON T146.F146_ID_CIA = T470.F470_ID_CIA
       AND T146.F146_ID_CONCEPTO = T470.F470_ID_CONCEPTO
       AND T146.F146_ID = T470.F470_ID_MOTIVO
     INNER JOIN T145_MC_CONCEPTOS T145
        ON T145.F145_ID = T146.F146_ID_CONCEPTO
     WHERE T350.F350_IND_ESTADO = 1
       AND T350.F350_ID_CIA = 2
       AND T145.F145_ID_MODULO = 5
     GROUP BY T350.F350_ROWID,
              T350.F350_ID_CIA,
              T350.F350_FECHA,
              T461.F461_ROWID_TERCERO_FACT,
              T461.F461_ID_SUCURSAL_FACT,
              T200.F200_NIT,
              T200.F200_RAZON_SOCIAL,
              T350.F350_ID_CO,
              T470.F470_ID_CO_MOVTO,
              T350.F350_ID_TIPO_DOCTO,
              T350.F350_CONSEC_DOCTO,
              T106_005.F106_DESCRIPCION,
              T106_035.F106_DESCRIPCION,
              T470.F470_IND_NATURALEZA,
              T461.F461_IND_NAT,
              SA.DEBITOS,
              SA.CREDITOS
    """