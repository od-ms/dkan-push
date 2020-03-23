$(function(){

    $.getJSON('muenster-dkan.json', function(dkanStats) { 

        // dkanStats.counts;
        // dkanStats.datasets;
        // dkanStats.groups;

        var dataSource = [];
        var obj = dkanStats.counts

        var keysSorted = Object.keys(obj).sort(function(a,b){return obj[a]-obj[b]})

        keysSorted.forEach(function(prop) {
            dataSource.push({
                source: prop,
                count: obj[prop]
            })
            console.log("o." + prop + " = " + obj[prop]);

        })

        $("#chart1").dxChart({
            dataSource: dataSource, 
            rotated: true,
            legend: {
                position: "inside",
                verticalAlignment: "bottom"
            },
            series: {
                argumentField: "source",
                valueField: "count",
                name: "Anzahl Open-Data-Datensätze",
                type: "bar"
             //   color: '#ffaa66'
            }
        });

        var datasets = dkanStats.datasets;
        $("#chart2").html('<table id="customers"><tr><th>Amt</th><th>Open-Data-Datensätze</th></table>')
        keysSorted.forEach(function(prop) {
            var html = '<tr><td>' + prop + '</td><td>';
            console.log(prop, datasets[prop])
            datasets[prop].forEach(function(dataset) {
                html += dataset + ', '
            });
            html += '</td></tr>';
            $('#customers').append(html);
        })
        

        /**
         * Paint external datasources graph
         */
        dataSource = [];
        obj = dkanStats.groups
        keysSorted = Object.keys(obj).sort(function(a,b){return obj[a]-obj[b]})
        keysSorted.forEach(function(prop) {
            dataSource.push({
                source: prop,
                count: obj[prop]
            })
            console.log("o." + prop + " = " + obj[prop]);
        })
        $("#chart3").dxChart({
            dataSource: dataSource, 
            rotated: true,
            legend: {
                position: "inside",
                verticalAlignment: "bottom"
            },
            series: {
                argumentField: "source",
                valueField: "count",
                name: "Anzahl Open-Data-Datensätze",
                type: "bar"
             //   color: '#ffaa66'
            }
        });



    }); 

});