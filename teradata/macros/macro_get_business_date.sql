/*******************************************************************************
 * macro_get_business_date
 *
 * Teradata MACRO that returns the previous business date, skipping weekends
 * and UK bank holidays. Uses the DIM_DATE calendar table.
 ******************************************************************************/

REPLACE MACRO BARCLAYS_DWH.macro_get_business_date (
    p_reference_date DATE DEFAULT CURRENT_DATE
) AS (
    SELECT MAX(dd.calendar_date) AS business_date
    FROM BARCLAYS_DWH.DIM_DATE dd
    WHERE dd.calendar_date < :p_reference_date
      AND dd.is_business_day = 'Y';
);
