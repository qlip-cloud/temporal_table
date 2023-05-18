drop procedure if exists load_journal_entry;
delimiter //
CREATE PROCEDURE load_journal_entry()
BEGIN

    DECLARE je_usr varchar(140) DEFAULT 'Administrator';
    DECLARE je_currency varchar(140) DEFAULT 'COP';

    DECLARE je_finance_book varchar(140);
    DECLARE tmp_finance_book varchar(140);
    DECLARE fb_valid int;
    
    DECLARE je_entry_type varchar(140);
    DECLARE je_is_opening varchar(140);

    DECLARE je_series varchar(140);
    DECLARE je_series_default varchar(140);
    DECLARE je_series_name varchar(140);
    DECLARE je_company varchar(140);
    DECLARE je_posting_date varchar(140);
    DECLARE je_title varchar(140);
    DECLARE je_date datetime(6);

    DECLARE gle_fiscal_year varchar(140) DEFAULT '';

    DECLARE je_total_debit decimal(18,2);
    DECLARE je_total_credit decimal(18,2);

    DECLARE je_name varchar(100);

    DECLARE canti1 int;
    DECLARE canti2 int;
    DECLARE canti3 int;
    DECLARE canti4 int;
    DECLARE canti5 decimal(18,2);
    DECLARE canti6 int;
    DECLARE canti7 int;
    DECLARE canti8 int;

    DECLARE jea_debit decimal(18,2);
    DECLARE jea_credit decimal(18,2);

    DECLARE je_valid int;
    DECLARE jea_valid int;
    DECLARE je_company_entry_type int;
    DECLARE temporal_total_lines int;

    DECLARE msg_valid varchar(140);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SHOW ERRORS;
        ROLLBACK;
    END;
    START TRANSACTION;

    -- Número de registros a procesar
    select count(*)
        into temporal_total_lines
        from `tabJournal_Entry_Temporal` jet;

    -- Validaciones Journal Entry en base al proceso que lanza la carga
    select count(*)
        into je_company_entry_type
        from `tabJournal_Entry_Temporal` jet
        inner join `tabqp_Advanced_Integration` adv_int
            on adv_int.status = 'Active' and adv_int.company = jet.company and adv_int.journal_type = jet.entry_type;

    -- Se asume que el archivo contiene lo que se va a cargar en un asiento
    -- SELECT '** Obtener datos de cabecera' AS '** DEBUG:';
    select jet.finance_book, jet.entry_type, jet.series, jet.company, jet.posting_date, jet.title, jet.total_debit, jet.total_credit
        into je_finance_book, je_entry_type, je_series, je_company, je_posting_date, je_title, je_total_debit, je_total_credit
        from `tabJournal_Entry_Temporal` jet
	    inner join `tabCompany` comp on  jet.company = comp.name;
    
    -- Se identifica si es un asiento de apertura
    if je_entry_type = 'Opening Entry' then
        SET je_is_opening = 'Yes';
    else
        SET je_is_opening = 'No';
    end if;

    -- Se coloca la serie por defecto para la carga de diarios por ésta vía
    SET je_series_default = 'ACC-JV-.YYYY.-';

    Select CONCAT('ACC-JV-',YEAR(now()),'-')
        into je_series_name;


    -- SELECT '** Obtener finance book' AS '** DEBUG:';

    -- Se busca el libro de finanzas por defecto en caso de venir vacío en el archivo
    if je_finance_book is null or je_finance_book = '' then
        select comp.default_finance_book
            into tmp_finance_book
            from tabCompany as comp
        where comp.name = je_company;
        SET fb_valid = 1;

    else
        select fb.name
            into tmp_finance_book
            from `tabFinance Book` as fb
        where fb.name = je_finance_book;

        if tmp_finance_book is null or tmp_finance_book = '' then
            SET fb_valid = 0;
        else
            SET fb_valid = 1;
        end if;

    end if;

    -- SELECT tmp_finance_book AS '** DEBUG:-------';


    -- SELECT '** Validar datos' AS '** DEBUG:';

    select fy.name
        into gle_fiscal_year
        from `tabFiscal Year` fy
    where disabled = 0 and je_posting_date between fy.year_start_date and fy.year_end_date
    order by fy.year_start_date desc
    LIMIT 1;

    -- TODO: Incorporar validaciones cuando se genere la serie
    if fb_valid = 1 and je_company_entry_type = 1 and gle_fiscal_year != '' and je_total_debit = je_total_credit and je_series_default = je_series then
        SET je_valid = 1;
    else
        SET je_valid = 0;
    end if;

    -- Validaciones Journal Entry Account

    select count(*)
        into canti1
        from `tabJournal_Entry_Temporal`
    where BINARY party_type not in (select name from `tabParty Type`);

    select count(*)
        into canti2
        from `tabJournal_Entry_Temporal`
    where party_type = 'Customer' and BINARY party not in (select name from tabCustomer);

    select count(*)
        into canti3
        from `tabJournal_Entry_Temporal`
    where party_type = 'Supplier' and BINARY party not in (select name from tabSupplier);

    select count(*)
        into canti4
        from `tabJournal_Entry_Temporal`
    where BINARY account not in (select name from tabAccount);

    select
        CAST(sum(debit_in_account_currency) - sum(credit_in_account_currency) AS DECIMAL(18,2)) as tot,
        sum(debit_in_account_currency) as debit, sum(credit_in_account_currency) as credit
        into canti5, jea_debit, jea_credit
        from `tabJournal_Entry_Temporal`;
    
    Select count(*)
        into canti6
        from (
        select account, sum(debit_in_account_currency) - sum(credit_in_account_currency) as result from `tabJournal_Entry_Temporal` as jet
        inner join `tabAccount` as acc on jet.account = acc.name and acc.balance_must_be = "Debit"
        group by account
        ) tlb
        where tlb.result < 0;
    
    Select count(*)
        into canti7
        from (
        select account, sum(debit_in_account_currency) - sum(credit_in_account_currency) as result from `tabJournal_Entry_Temporal` as jet
        inner join `tabAccount` as acc on jet.account = acc.name and acc.balance_must_be = "Credit"
        group by account
        ) tlb
        where tlb.result > 0;

    Select count(*)
        into canti8
        FROM tabJournal_Entry_Temporal
        WHERE debit_in_account_currency = credit_in_account_currency;

    if canti1=0 && canti2=0 && canti3=0 && canti4=0 && canti5=0.00 && canti6=0 && canti7=0 && canti8=0 then
        SET jea_valid = 1;
    else
        SET jea_valid = 0;
    end if;

    if je_valid=1 && jea_valid=1 && je_total_debit=jea_debit then
        -- insert
        UPDATE `tabSeries` SET `current` = `current` + 1 WHERE `name`=je_series_name;

        Select CONCAT(je_series_name, LPAD(current, 5, 0)) into je_name from  `tabSeries` WHERE `name`=je_series_name;

        -- SELECT '** Inicio carga de Journal Entry' AS '** DEBUG:', NOW() AS '** HORA INICIO';

        select cast(NOW() as datetime(6)) into je_date;

        INSERT INTO `tabJournal Entry` (
            `name`,`finance_book`,`company`,`posting_date`,`title`,`total_debit`,`total_credit`,
            `creation`,`modified`,`modified_by`,`owner`,`naming_series`,
            `total_amount_currency`,`docstatus`,`total_amount`, `voucher_type`, `is_opening`)
            select 
            je_name as `name`,
            tmp_finance_book as `finance_book`,
            je_company as `company`,
            je_posting_date as `posting_date`,
            je_title `title`,
            je_total_debit as `total_debit`,
            je_total_credit as `total_credit`,
            je_date as `creation`,
            je_date as `modified`,
            je_usr as `modified_by`,
            je_usr as `owner`,
            je_series_default as `naming_series`,
            je_currency as `total_amount_currency`,
            1 as `docstatus`,
            je_total_credit as `total_amount`,
            je_entry_type as `voucher_type`,
            je_is_opening as `is_opening`;


        -- NO esta incluido el centro de costo
        -- NOTA: SE DEJARÁ EN CAMPO against_account VACIO

        -- SELECT '** Inicio carga de Journal Entry Account' AS '** DEBUG:', NOW() AS '** HORA INICIO';

        INSERT INTO `tabJournal Entry Account` (
            `name`,`creation`,`modified`,`modified_by`,`owner`,
            `parent`,`parentfield`,`parenttype`,
            `account_currency`,`exchange_rate`,
            `debit_in_account_currency`,`debit`, `credit_in_account_currency`,`credit`,
            `account`,`account_type`,`party_type`,`party`,`is_advance`,`idx`,`docstatus`)
            select 
            `name`,`creation`,`modified`,`modified_by`,`owner`,
            `parent`,`parentfield`,`parenttype`,
            `account_currency`,`exchange_rate`,
            `debit_in_account_currency`,`debit`,`credit_in_account_currency`,`credit`,
            `account`,`account_type`,`party_type`,`party`,`is_advance`,`idx`,`docstatus`
            from (
                SELECT 
                SUBSTRING(SHA1(LCASE(CONCAT(je_name,temp.name))),31) as `name`,
                je_date as creation, je_date as `modified`,
                je_usr as modified_by, je_usr as `owner`,
                je_name as parent, 'accounts' as parentfield, 'Journal Entry' as `parenttype`,
                je_currency as account_currency,  1.0 as `exchange_rate`,
                temp.debit_in_account_currency as debit_in_account_currency, temp.debit_in_account_currency as `debit`,
                temp.credit_in_account_currency as credit_in_account_currency, temp.credit_in_account_currency as `credit`,
                temp.account as account, acc.account_type as `account_type`,
                temp.party_type as party_type, temp.party as `party`,
                'No' as `is_advance`,
                CAST(temp.name AS UNSIGNED) - 1 as `idx`,
                1 as `docstatus`
                from
                `tabJournal_Entry_Temporal` as temp
                inner join `tabAccount` as acc on temp.account = acc.name
            ) drb order by `idx`;


        -- SELECT '** Inicio carga de GL Entry' AS '** DEBUG:', NOW() AS '** HORA INICIO';

        INSERT INTO `tabGL Entry` (
            `name`, `owner`, `creation`, `modified`, `modified_by`, `idx`, `docstatus`,
            `finance_book`,
            `posting_date`, `account`, `party_type`, `party`,
            `debit`, `credit`, `account_currency`, `debit_in_account_currency`, `credit_in_account_currency`,
            `voucher_type`, `voucher_no`, `is_opening`, `is_advance`,
            `fiscal_year`, `company`, `to_rename`, `is_cancelled`)
            select
            `name`, `owner`, `creation`, `modified`, `modified_by`, `idx`, `docstatus`,
            `finance_book`,
            `posting_date`, `account`, `party_type`, `party`,
            `debit`, `credit`, `account_currency`, `debit_in_account_currency`, `credit_in_account_currency`,
            `voucher_type`, `voucher_no`, `is_opening`, `is_advance`,
            `fiscal_year`, `company`, `to_rename`, `is_cancelled`
            from (
                SELECT
                jea.name, jea.owner, jea.creation, jea.modified, jea.modified_by, 0 as idx, 1 as docstatus,
                je.finance_book,
                je.posting_date, jea.account, jea.party_type, jea.party,
                jea.debit, jea.credit, jea.account_currency, jea.debit_in_account_currency, jea.credit_in_account_currency,
                je.voucher_type, je.name as voucher_no,  je.is_opening, jea.is_advance,
                gle_fiscal_year as fiscal_year, je.company,  '1' as to_rename, 0 as is_cancelled
                FROM `tabJournal Entry Account` as jea
                INNER JOIN `tabJournal Entry` as je on jea.parent = je.name and jea.parent = je_name and jea.parentfield = 'accounts'
                order by jea.idx
            ) as drb;

        select temporal_total_lines as total_lines_processed;
        select je_name as document;
        select 1 as result;
    else

        if canti1 <> 0 then
            SELECT CONCAT(CAST(canti1 AS CHAR), " party type(s) no match. Is case sensitive.") as Party;
            select distinct party_type
                from `tabJournal_Entry_Temporal`
            where BINARY party_type not in (select name from `tabParty Type`);
        end if;

        if canti2 <> 0 then
            SELECT CONCAT(CAST(canti2 AS CHAR), " customer(s) no match. Check if the file is in UTF-8 format. Is case sensitive too.") as Customer;
            select distinct party
                from `tabJournal_Entry_Temporal`
            where party_type = 'Customer' and BINARY party not in (select name from tabCustomer);
        end if;

        if canti3 <> 0 then
            SELECT CONCAT(CAST(canti3 AS CHAR), " supplier(s) no match. Check if the file is in UTF-8 format. Is case sensitive too.") as Supplier;
            select distinct party
                from `tabJournal_Entry_Temporal`
            where party_type = 'Supplier' and BINARY party not in (select name from tabSupplier);
        end if;

        if canti4 <> 0 then
            SELECT CONCAT(CAST(canti4 AS CHAR), " account(s) no match. Check if the file is in UTF-8 format. Is case sensitive too.") as Account;
            select distinct account
                from `tabJournal_Entry_Temporal`
            where BINARY account not in (select name from tabAccount);
        end if;

        if canti5 <> 0 then
            select canti5 as `Difference in table`;
            select jea_debit as debit_sum;
            select jea_credit as credit_sum;
        end if;

        if canti6 <> 0 then
            SELECT CONCAT(CAST(canti6 AS CHAR), " accounts: Balance must be Debit") as `Balance must be Debit`;
        end if;

        if canti7 <> 0 then
            SELECT CONCAT(CAST(canti7 AS CHAR), " accounts: Balance must be Credit") as `Balance must be Credit`;
        end if;

        if canti8 <> 0 then
            SELECT CONCAT(CAST(canti8 AS CHAR), " lines with debit and credit having the same value.") as ` Validate value in debit and credit`;
        end if;

        if je_valid = 0 then

            if fb_valid = 0 then
                Select "Finance book does not exist." as `Finance Book Result`;
            end if;

            if je_company_entry_type != 1 then
                Select "There is no correspondence between company/type of journal with the document that initiates the process." as `Difference with company or entry_type`;
                select je_company as company;
                select je_entry_type as entry_type;
            end if;

            if gle_fiscal_year = '' then
                select je_posting_date as posting_date;
                select gle_fiscal_year as fiscal_year;
            end if;

            if je_total_debit != je_total_credit then
                select je_total_debit as debit_column;
                select je_total_credit as credit_column;
                select jea_debit as `Total calculado`;
            end if;

            if je_series_default != je_series then
                Select je_series_default as `Default serie`;
                select je_series as serie_csv;
            end if;

        end if;

        TRUNCATE TABLE  `tabJournal_Entry_Temporal`;
        select temporal_total_lines as `Total lines processed`;
        select 0 as Result;
    end if;

    COMMIT;
    -- SELECT '** Finalización' AS '** DEBUG:', NOW() AS '** HORA FIN';
END //
delimiter ;

-- load journal entry
call load_journal_entry();
