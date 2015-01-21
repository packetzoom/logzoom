package websocket

var index = `
<!DOCTYPE html>
<html>
 <head>
  <meta charset="UTF-8" />
  <title>Logs</title>
  <style>
    body {
      padding: 40px;
      font-family: Consolas, Menlo, Monaco, Lucida Console, Courier New, monospace, serif;
    }
    ul {
      list-style: none;
      padding: 0;
    }
  </style>
 </head>
 <body>
  <h1>Log Files</h1>
  <ul>
    {{range $source, $exists := .}}
    <li><a href="/logs?source={{$source}}">{{$source}}</a></li>
    {{end}}
  </ul>
 </body>
</html>
`
var logs = `
<!DOCTYPE html>
<html>
 <head>
  <meta charset="UTF-8" />
  <title>{{.Source}}</title>
  <style>
    body {
      padding: 40px;
      font-family: Consolas, Menlo, Monaco, Lucida Console, Courier New, monospace, serif;
    }
    ul#msg-list {
      list-style-type: none;
      overflow: auto;
      padding: 0;
    }
  </style>
  <script src="//ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
  <script>
   $(function() {
    var $ul = $('#msg-list');
    var ws = new WebSocket("{{.Server}}");
    ws.onmessage = function(e) {
     $('<li>').text(e.data).appendTo($ul);
     $($ul).scrollTop($($ul)[0].scrollHeight);
    };
   });
  </script>
 </head>
 <body>
  <h1>{{.Source}}</h1>
  <ul id="msg-list"></ul>
 </body>
</html>
`
