SHOW databases;

USE order_prediction_db;
SHOW TABLES;
SELECT * FROM suggested_order;
SELECT * FROM suggested_order where repid="2057"

SELECT DISTINCT repid, COUNT(*) AS inserted_rows
FROM suggested_order
GROUP BY repid
ORDER BY repid;

Select * from modified_suggested_order
Select * from modified_predictions
SELECT product, Qty, Next_Visit_Date FROM suggested_order WHERE outletid = 'HO0487_NSF'
select * from sales_flat;
select * from payments

select ex.Code,ex.Name 
from payments p 
inner join external_parties ex on p.AssignedBranchId = ex.Id
where Type = 0 and Deleted = 0 and PaymentType in (0,1);

select ex.Code,ex.Name 
from payments p 
inner join external_parties ex on p.AssignedBranchId = ex.Id
where p.Type = 0 and Deleted = 0 and PaymentType in (0,1);

-- select * from payments where Type = 0 and Deleted = 0 and PaymentType in (1)
select * from external_parties

select `payments`.`Id`,`payments`.`Type`,`payments`.`SerialNo`,`payments`.`Date`,`payments`.`Category`,`payments`.`Amount`,`payments`.`ChequeDate`,`payments`.`PaymentType`,`payments`.`ClassType`,`payments`.`Version`,`payments`.`PackageCode`,`payments`.`Payee`,`payments`.`Method`,`payments`.`RefNo`,`payments`.`Bank`,`payments`.`Branch`,`payments`.`Remarks`,`payments`.`Status`,`payments`.`Deposited`,`payments`.`SentToBank`,`payments`.`ChequeNo`,`payments`.`AccountPayee`,`payments`.`NonNegotiable`,`payments`.`BillPayee`,`payments`.`ModifiedDate`,`payments`.`Deleted`,`payments`.`DeletedReason`,`payments`.`DeletedDate`,`payments`.`Reason`,`payments`.`PaymentApprovalStatus`,`payments`.`PaymentLocation`,`payments`.`ApprovedDate`,`payments`.`ChequeTransferDate`,`payments`.`IsHP`,`payments`.`UUID`,`payments`.`InvoiceId`,`payments`.`OtherPartyId`,`payments`.`EnterdById`,`payments`.`NoteAssociationId`,`payments`.`RefundId`,`payments`.`ParentPaymentId`,`payments`.`ParentNoteId`,`payments`.`CashBookAccountId`,`payments`.`DepositAccountId`,`payments`.`BankAccountId`,`payments`.`SubAnalysisId`,`payments`.`CollectorId`,`payments`.`ModifiedById`,`payments`.`DeletedById`,`payments`.`PaymentAssociationId`,`payments`.`OverPaySettlementId`,`payments`.`PaymentBranchId`,`payments`.`ExpenseCategoryId`,`payments`.`ApprovedById`,`payments`.`ChequeTransferById`,`payments`.`TillId`,`payments`.`CompanyId`,`payments`.`TerritoryId`,`payments`.`NodeId`,`payments`.`SalesHierarchySnapshotId`,`payments`.`ImagePath`,`invoices`.`Id` as `invoices_Id`,`invoices`.`BranchId`,`external_parties`.`Code`as `Distributor_Code`,`external_parties`.`Name`,`external_parties`.`IsDistributor`,`external_parties`.`Type` as `external_parties_Type` from ((`payments` `payments`
 inner join `invoices` `invoices` on (`invoices`.`Id` = `payments`.`InvoiceId`))
 inner join `external_parties` `external_parties` on (`external_parties`.`Id` = `invoices`.`BranchId`))
where ((`payments`.`Date` >= ?startDate) and (`payments`.`Date` <= ?EndDate) and (`external_parties`.`Type` = 2)) and(`payments`.Type = 0 )and (`payments`.Deleted = 0 )and (`payments`.PaymentType in (0,1))

select `Sales_Flat`.*,`sales_hierarchy_nodes`.`Code`,`sales_hierarchy_nodes`.`Name`,`sales_targets`.`ProductId`,`sales_targets`.`CustomerId`,`sales_targets`.`RepId`,`sales_targets`.`StartDate`,`sales_targets`.`EndDate`,`sales_targets`.`Qty`,`sales_targets`.`Value`,`sales_hierarchy_nodes`.`Id` from ((`Sales_Flat` `Sales_Flat`
 inner join `sales_hierarchy_nodes` `sales_hierarchy_nodes` on (`sales_hierarchy_nodes`.`Code` = `Sales_Flat`.`RepCode`))
 inner join `sales_targets` `sales_targets` on (`sales_targets`.`RepId` = `sales_hierarchy_nodes`.`Id`))
 
 
 
 select `Sales_Flat`.*,`sales_hierarchy_nodes`.`Code`,`sales_hierarchy_nodes`.`Name`,`sales_targets`.`ProductId`,`sales_targets`.`CustomerId`,`sales_targets`.`RepId`,`sales_targets`.`StartDate`,`sales_targets`.`EndDate`,`sales_targets`.`Qty`,`sales_targets`.`Value` from ((`Sales_Flat` `Sales_Flat`
 inner join `sales_hierarchy_nodes` `sales_hierarchy_nodes` on (`sales_hierarchy_nodes`.`Id` = `Sales_Flat`.`RepId`))
 inner join `sales_targets` `sales_targets` on (`sales_targets`.`RepId` = `sales_hierarchy_nodes`.`Id`))
where ((`Sales_Flat`.`Date` >= '2025-08-01') and (`Sales_Flat`.`Date` <= '2025-09-30') and (`sales_targets`.`StartDate` >= '2025-08-01') and (`sales_targets`.`EndDate` <= '2025-09-30')) 

select `Sales_Flat`.*,`sales_hierarchy_nodes`.`Code`,`sales_hierarchy_nodes`.`Name`,`sales_targets`.`RepId`,`sales_targets`.`StartDate`,`sales_targets`.`EndDate`,`sales_targets`.`Qty`,`sales_targets`.`Value` from ((`Sales_Flat` `Sales_Flat`
 inner join `sales_hierarchy_nodes` `sales_hierarchy_nodes` on (`sales_hierarchy_nodes`.`Id` = `Sales_Flat`.`RepId`))
 inner join `sales_targets` `sales_targets` on (`sales_targets`.`RepId` = `sales_hierarchy_nodes`.`Id`))
 
 
 select `Sales_Flat`.*,`sales_hierarchy_nodes`.`Code`,`sales_hierarchy_nodes`.`Name`,`sales_targets`.`RepId`,`sales_targets`.`StartDate`,`sales_targets`.`EndDate`,`sales_targets`.`Qty`,`sales_targets`.`Value` from ((`Sales_Flat` `Sales_Flat`
 inner join `sales_hierarchy_nodes` `sales_hierarchy_nodes` on (`sales_hierarchy_nodes`.`Id` = `Sales_Flat`.`RepId`))
 inner join `sales_targets` `sales_targets` on (`sales_targets`.`RepId` = `sales_hierarchy_nodes`.`Id`))
where (`Sales_Flat`.`Date` >= '2025-08-01') and (`Sales_Flat`.`Date` <= '2025-09-30')

select `Sales_Flat`.`Date`,`Sales_Flat`.`InvoiceNo`,`Sales_Flat`.`ProductCode`,`Sales_Flat`.`ProductName`,`Sales_Flat`.`Qty`,`Sales_Flat`.`Discount`,`Sales_Flat`.`NetValue`,`Sales_Flat`.`Brand`,`Sales_Flat`.`CustomerCategory`,`Sales_Flat`.`ProductCategory`,`Sales_Flat`.`Route`,`Sales_Flat`.`RepId`,`Sales_Flat`.`RepCode`,`Sales_Flat`.`RepName`,`Sales_Flat`.`CustomerCode`,`Sales_Flat`.`CustomerName`,`Sales_Flat`.`ASM`,`Sales_Flat`.`RSM`,`Sales_Flat`.`Distributor`,`Sales_Flat`.`Type`,`sales_targets`.`Value`,`sales_targets`.`Qty` as `sales_targets_Qty` from (`sales_targets` `sales_targets`
 inner join `Sales_Flat` `Sales_Flat` on (`Sales_Flat`.`RepId` = `sales_targets`.`RepId`))
where ((`Sales_Flat`.`Date` >= '2025-08-01') and (`Sales_Flat`.`Date` <= '2025-08-15')) 

select `sales_hierarchy_nodes`.`Code`,`sales_hierarchy_nodes`.`Name`,`sales_targets`.`RepId`,sum(`sales_targets`.`Qty`) as `Qty_Sum`,sum(`sales_targets`.`Value`) as `Value_Sum`,`sales_targets`.`StartDate`,`Sales_Flat`.`Date`,`Sales_Flat`.`InvoiceNo`,`Sales_Flat`.`ProductName`,sum(`Sales_Flat`.`Qty`) as `Qty_Sum_1`,sum(`Sales_Flat`.`Discount`) as `Discount_Sum`,sum(`Sales_Flat`.`NetValue`) as `NetValue_Sum`,`Sales_Flat`.`Brand`,`Sales_Flat`.`CustomerCategory`,`Sales_Flat`.`ProductCategory`,`Sales_Flat`.`Route`,`Sales_Flat`.`RepId` as `Sales_Flat_RepId`,`Sales_Flat`.`RepCode`,`Sales_Flat`.`RepName`,`Sales_Flat`.`CustomerCode`,`Sales_Flat`.`CustomerName`,`Sales_Flat`.`ASM`,`Sales_Flat`.`RSM`,`Sales_Flat`.`Distributor`,`Sales_Flat`.`Type` from ((`Sales_Flat` `Sales_Flat`
 inner join `sales_hierarchy_nodes` `sales_hierarchy_nodes` on (`sales_hierarchy_nodes`.`Id` = `Sales_Flat`.`RepId`))
 inner join `sales_targets` `sales_targets` on (`sales_targets`.`RepId` = `sales_hierarchy_nodes`.`Id`))
where ((`Sales_Flat`.`Date` >= '2025-09-01') and (`Sales_Flat`.`Date` <= '2025-09-30') and (`sales_targets`.`StartDate` >= '2025-09-01') and (`sales_targets`.`EndDate` <= '2025-09-30'))
group by `sales_targets`.`RepId`,`sales_hierarchy_nodes`.`Code`,`sales_hierarchy_nodes`.`Name`,`sales_targets`.`StartDate`,`sales_hierarchy_nodes`.`Id`,`Sales_Flat`.`Date`,`Sales_Flat`.`InvoiceNo`,`Sales_Flat`.`ProductName`,`Sales_Flat`.`Brand`,`Sales_Flat`.`CustomerCategory`,`Sales_Flat`.`ProductCategory`,`Sales_Flat`.`Route`,`Sales_Flat`.`RepId`,`Sales_Flat`.`RepCode`,`Sales_Flat`.`RepName`,`Sales_Flat`.`CustomerCode`,`Sales_Flat`.`CustomerName`,`Sales_Flat`.`ASM`,`Sales_Flat`.`RSM`,`Sales_Flat`.`Distributor`,`Sales_Flat`.`Type` 