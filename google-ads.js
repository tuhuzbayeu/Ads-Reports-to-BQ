function main() {
  
  // Параметры
  
  var projectId = '--';
  var dataSetId = '--';
  var tableId = '--';
  
  var dateYear = Utilities.formatDate(new Date(), 'Europe/Moscow', 'yyyy');
  var dateMonth = Utilities.formatDate(new Date(), 'Europe/Moscow', 'MM');
  
  tableId = tableId + '_' + dateMonth + dateYear;
  
  
  // Сбор данных
  
  var insertAllRequest = BigQuery.newTableDataInsertAllRequest();
  insertAllRequest.rows = [];
  
  var i = 1;
  var accountIterator = AdsManagerApp.accounts()
      .withCondition('Impressions > 0')
      .forDateRange('THIS_MONTH')
      .get();
  while (accountIterator.hasNext()) {
    var account = accountIterator.next();
    var accountID = account.getCustomerId() ? account.getCustomerId() : '--';
    var accountName = account.getName() ? account.getName() : '--';
    AdsManagerApp.select(account);
        
    var query = 'SELECT Date, CampaignName, Clicks, Impressions, VideoViews, Conversions, Cost '
              + 'FROM CAMPAIGN_PERFORMANCE_REPORT '
              + 'WHERE Impressions > 0 '
              + 'DURING THIS_MONTH';
    var report = AdWordsApp.report(query);
    var rows = report.rows();

    while (rows.hasNext()) {
      var row = rows.next();
      var date = Utilities.formatDate(new Date(row['Date']), 'Europe/Moscow', 'yyyy-MM-dd');
      var CampaignName = row['CampaignName'];
      var Clicks = parseFloat(row['Clicks'].replace(',', ''));
      var Impressions = parseFloat(row['Impressions'].replace(',', ''));
      var Views = parseFloat(row['VideoViews'].replace(',', ''));
      var Conversions = parseFloat(row['Conversions'].replace(',', ''));
      var Cost = parseFloat(row['Cost'].replace(',', ''));
      
      var data = BigQuery.newTableDataInsertAllRequestRows();
      data.insertId = i;
      data.json = {
        'accountID': accountID,
        'accountName': accountName,
        'Date': date,
        'CampaignName': CampaignName,
        'Clicks': Clicks,
        'Impressions': Impressions,
        'Views': Views,
        'Conversions': Conversions,
        'Cost': Cost
      };
      
      insertAllRequest.rows.push(data);
           
      i = i + 1;
    }  
  }
  
  
  // Удаление и создание таблицы
  
  var tables = BigQuery.Tables.list(projectId, dataSetId);
  var tableExists = false;
  if (tables.tables != null) {
    for (var i = 0; i < tables.tables.length; i++) {
      var table = tables.tables[i];
      if (table.tableReference.tableId == tableId) {
        tableExists = true;
        break;
      }
    }
  }
  
  if (tableExists) BigQuery.Tables.remove(projectId, dataSetId, tableId);
   
  var table = BigQuery.newTable();
  var schema = BigQuery.newTableSchema();

  var accountIDFieldSchema = BigQuery.newTableFieldSchema();
  accountIDFieldSchema.description = 'accountID';
  accountIDFieldSchema.name = 'accountID';
  accountIDFieldSchema.type = 'STRING';

  var accountNameFieldSchema = BigQuery.newTableFieldSchema();
  accountNameFieldSchema.description = 'accountName';
  accountNameFieldSchema.name = 'accountName';
  accountNameFieldSchema.type = 'STRING';

  var DateFieldSchema = BigQuery.newTableFieldSchema();
  DateFieldSchema.description = 'Date';
  DateFieldSchema.name = 'Date';
  DateFieldSchema.type = 'DATE';

  var CampaignNameFieldSchema = BigQuery.newTableFieldSchema();
  CampaignNameFieldSchema.description = 'CampaignName';
  CampaignNameFieldSchema.name = 'CampaignName';
  CampaignNameFieldSchema.type = 'STRING';

  var ClicksFieldSchema = BigQuery.newTableFieldSchema();
  ClicksFieldSchema.description = 'Clicks';
  ClicksFieldSchema.name = 'Clicks';
  ClicksFieldSchema.type = 'FLOAT';

  var ImpressionsFieldSchema = BigQuery.newTableFieldSchema();
  ImpressionsFieldSchema.description = 'Impressions';
  ImpressionsFieldSchema.name = 'Impressions';
  ImpressionsFieldSchema.type = 'FLOAT';

  var ViewsFieldSchema = BigQuery.newTableFieldSchema();
  ViewsFieldSchema.description = 'Views';
  ViewsFieldSchema.name = 'Views';
  ViewsFieldSchema.type = 'FLOAT';

  var ConversionsFieldSchema = BigQuery.newTableFieldSchema();
  ConversionsFieldSchema.description = 'Conversions';
  ConversionsFieldSchema.name = 'Conversions';
  ConversionsFieldSchema.type = 'FLOAT';

  var CostFieldSchema = BigQuery.newTableFieldSchema();
  CostFieldSchema.description = 'Cost';
  CostFieldSchema.name = 'Cost';
  CostFieldSchema.type = 'FLOAT';

  schema.fields = [
    accountIDFieldSchema, accountNameFieldSchema, DateFieldSchema, CampaignNameFieldSchema, ClicksFieldSchema, ImpressionsFieldSchema, ViewsFieldSchema, ConversionsFieldSchema, CostFieldSchema
  ];

  table.schema = schema;
  table.id = tableId;
  table.friendlyName = tableId;

  table.tableReference = BigQuery.newTableReference();
  table.tableReference.datasetId = dataSetId;
  table.tableReference.projectId = projectId;
  table.tableReference.tableId = tableId;

  table = BigQuery.Tables.insert(table, projectId, dataSetId);
  
  
  
  // Импорт данных
  
  var result = BigQuery.Tabledata.insertAll(insertAllRequest, projectId,
      dataSetId, tableId);
  
  
  
  // Результат
  
  if (result.insertErrors != null) {
    var allErrors = [];

    for (var i = 0; i < result.insertErrors.length; i++) {
      var insertError = result.insertErrors[i];
      allErrors.push(Utilities.formatString('Error inserting item: %s',
                                            insertError.index));

      for (var j = 0; j < insertError.errors.length; j++) {
        var error = insertError.errors[j];
        allErrors.push(Utilities.formatString('- ' + error));
      }
    }
    Logger.log(allErrors.join('\n'));
  } else {
    Logger.log(Utilities.formatString('%s data rows inserted successfully.',
                                      insertAllRequest.rows.length));
  }
  
}
