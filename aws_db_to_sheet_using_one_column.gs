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
        
        var bookings = runQuery_bookings(sqlQuery) ;
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
        
        var bookings = runQuery_bookings(sqlQuery) ;
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
      
      var bookings = runQuery_bookings(sqlQuery) ;
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
    
    var bookings = runQuery_bookings(sqlQuery) ;
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

function runQuery_bookings(queryString) {
  var address = 'DB_ADDRESS.cbdetcjaymd6.us-east-1.rds.amazonaws.com'
  var user = 'USERNAME' ; 
  var userPassword = 'USER_PASSWORD' ; 
  var database = 'DATABASE_NAME' ; 
  var databaseUrl = 'jdbc:mysql://' + address + '/' + database ; 
  var conn = Jdbc.getConnection(databaseUrl, user, userPassword) ; 
  var stmt = conn.createStatement() ;
  var results = stmt.executeQuery(queryString) ;
  var numColumns = results.getMetaData().getColumnCount() ; 
  var matrix = [] ; 
  
  while (results.next()) {
    var row = [] ;
    for (var i = 1; i <= numColumns; ++i) {
      var column = results.getString(i) ; 
      row.push(column) ;
    }
    matrix.push(row) ; 
  }
  results.close() ; 
  stmt.close() ; 
  conn.close() ; 
  
  return matrix
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
