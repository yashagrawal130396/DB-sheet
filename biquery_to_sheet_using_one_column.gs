function dbdata() {
  var sheets = SpreadsheetApp.openById('SHEET_ID').getSheetByName('COLUMN_SHEET_NAME');
  var sheet2 = SpreadsheetApp.openById('SHEET_ID').getSheetByName('PASTE_SHEET_NAME');
  var count_ids = getNextRowdb(sheets);
  //  var count_ids = 50 ;
  if(count_ids >= 1000) {
    var page1 = Math.floor(count_ids/1000);
    for(var i = 0; i < page1; i++){
      if(i == 0) {
        var j = 1;
        var j2 = 1000;
        var nRR = sheet2.getLastRow() + 1;
        
        var num_rows = j2;
        var range = sheets.getRange(j+1, 1, num_rows, 1).getValues();
        var combined = range.join(",");
        var sqlQuery = '\
Select booking_id, ticket_tags, booking_status, ticket_completion_status, customer_email, price_payable_usd, ticket_id, itinerary_id from `segment-data.analytics_prod.bookings` \
Where booking_id in(' + combined + ')';
        
        var bookings = queryBigQuery(sqlQuery) ;
        sheets.getRange(j+1, 2, num_rows, 1).setValue('Fetched');
        
        var matrix = [] // to add to the sheet 
        for (var k =0; k < bookings.length; ++k) {
          var id = bookings[k][0] ;
          var tags = bookings[k][1] ;
          var status = bookings[k][2] ;
          var net = bookings[k][3] ;
          var currency = bookings[k][4] ;
          var price = bookings[k][5]
          var ticketid = bookings[k][6] ;
          var itid = bookings[k][7] ;
          
          var cases = [[id],[tags],[status],[net],[currency],[price],[ticketid],[itid]];
          matrix.push(cases);
        }
        sheet2.getRange(nRR, 1, matrix.length, matrix[0].length).setValues(matrix);
      } else {
        var j = 1 + (1000*i);
        var j2 = 1000;
        var nR = sheet2.getLastRow() + 1;
        
        var num_rows = j2;
        var range = sheets.getRange(j+1, 1, num_rows, 1).getValues();
        var combined = range.join(",");
        var sqlQuery = '\
Select booking_id, ticket_tags, booking_status, ticket_completion_status, customer_email, price_payable_usd, ticket_id, itinerary_id from `segment-data.analytics_prod.bookings` \
Where booking_id in(' + combined + ')';
        
        var bookings = queryBigQuery(sqlQuery) ;
        sheets.getRange(j+1, 2, num_rows, 1).setValue('Fetched');
        
        var matrix = [] // to add to the sheet 
        for (var k =0; k < bookings.length; ++k) {
          var id = bookings[k][0] ;
          var tags = bookings[k][1] ;
          var status = bookings[k][2] ;
          var net = bookings[k][3] ;
          var currency = bookings[k][4] ;
          var price = bookings[k][5]
          var ticketid = bookings[k][6] ;
          var itid = bookings[k][7] ;
          
          var cases = [[id],[tags],[status],[net],[currency],[price],[ticketid],[itid]];
          matrix.push(cases);
        }
        sheet2.getRange(nR, 1, matrix.length, matrix[0].length).setValues(matrix);
      }
    }
    for(var i = page1; i<=page1; i++) {
      var j = 1 + (1000*i);
      var j2 = count_ids;
      var num_rows = j2 - (j-1);
      var nR2 = sheet2.getLastRow() + 1;
      var range = sheets.getRange(j+1, 1, num_rows, 1).getValues();
      var combined = range.join(",");
      var sqlQuery = '\
Select booking_id, ticket_tags, booking_status, ticket_completion_status, customer_email, price_payable_usd, ticket_id, itinerary_id from `segment-data.analytics_prod.bookings` \
Where booking_id in(' + combined + ')';
      
      var bookings = queryBigQuery(sqlQuery) ;
      sheets.getRange(j+1, 2, num_rows, 1).setValue('Fetched');
      
      var matrix = [] // to add to the sheet 
      for (var k =0; k < bookings.length; ++k) {
        var id = bookings[k][0] ;
        var tags = bookings[k][1] ;
        var status = bookings[k][2] ;
        var net = bookings[k][3] ;
        var currency = bookings[k][4] ;
        var price = bookings[k][5]
        var ticketid = bookings[k][6] ;
        var itid = bookings[k][7] ;
        
        var cases = [[id],[tags],[status],[net],[currency],[price],[ticketid],[itid]];
        matrix.push(cases);
      }
      sheet2.getRange(nR2, 1, matrix.length, matrix[0].length).setValues(matrix);
    }
  } else {
    var j = 1;
    var j2 = count_ids;
    var num_rows = j2;
    var nR3 = sheet2.getLastRow() + 1;
    var range = sheets.getRange(j+1, 1, num_rows, 1).getValues();
    var combined = range.join(",");
    var sqlQuery = '\
Select booking_id, ticket_tags, booking_status, ticket_completion_status, customer_email, price_payable_usd, ticket_id, itinerary_id from `segment-data.analytics_prod.bookings` \
Where booking_id in(' + combined + ')';
    
    var bookings = queryBigQuery(sqlQuery) ;
    sheets.getRange(j+1, 2, num_rows, 1).setValue('Fetched');
    
    var matrix = [] // to add to the sheet 
    for (var k =0; k < bookings.length; ++k) {
      var id = bookings[k][0] ;
      var tags = bookings[k][1] ;
      var status = bookings[k][2] ;
      var net = bookings[k][3] ;
      var currency = bookings[k][4] ;
      var price = bookings[k][5]
      var ticketid = bookings[k][6] ;
      var itid = bookings[k][7] ;
      
      var cases = [[id],[tags],[status],[net],[currency],[price],[ticketid],[itid]];
      matrix.push(cases);
    }
    sheet2.getRange(nR3, 1, matrix.length, matrix[0].length).setValues(matrix);
  }
  
}

function queryBigQuery(query_string) {
  var project_id = 'PROJECT_ID'; 
  var request = {
    query: query_string, 
    useLegacySql: false
  };
  var query_results = BigQuery.Jobs.query(request, project_id);
  var job_id = query_results.jobReference.jobId;
  // Check on status of the Query Job.
  var sleep_time_ms = 500;
  while (!query_results.jobComplete) {
    Utilities.sleep(sleep_time_ms);
    sleep_time_ms *= 2;
    query_results = BigQuery.Jobs.getQueryResults(project_id, job_id);
  }
  // Get all the rows of results.
  var rows = query_results.rows;
  while (query_results.pageToken) {
    query_results = BigQuery.Jobs.getQueryResults(project_id, job_id, {
      pageToken: query_results.pageToken
    });
    rows = rows.concat(query_results.rows);
  }
  if (rows) {
    // Append the results.
    var data = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
      var cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (var j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    }
    return data ; 
  } else {
    return null ;
  }  
}


function getNextRowdb(sheets) {  
  var bookingids = sheets.getRange("A2:A").getValues();
  for (var i in bookingids) {
    if(bookingids[i][0] == "") {
      Logger.log(Number(i));
      return Number(i);
      break;
    }
  }
}
