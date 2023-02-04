const fs = require('fs')

let d = new Date("DLKJH DKLJHD ")
if ("" + d == "Invalid Date") {
    console.log("invalid date")
}
process.exit(1)

let buffer = fs.readFileSync('timestamps.txt')

// read line by line
let lines = buffer.toString().split('\n')

function parseDate(line) {
    try {
        line = line.trim()
        line = line.replace(/[^\x00-\x7F]/g, "")

        if (line.length == 0)
            return null

        // test if line has only digits
        if (line.match(/^\d+$/)) {
            let timestamp = parseInt(line)
            if (timestamp < 315532990) {
                return null
            }
            if (Math.abs(Date.now() - timestamp) < Math.abs(Date.now() - timestamp * 1000)) {
                //console.log(`ms ${timestamp}`);
            } else {
                //console.log(`s ${timestamp}`);
                timestamp *= 1000
            }
            if (timestamp > Date.now()) {
                return null
            }

            let date = new Date(timestamp)
            if (date != "Invalid Date") {
                return date
            }
        }

        if (line.length == 10 && line[4] == ':' && line[7] == ':') {
            if (line.substring(0, 2) == "01")
                line = "20" + line.substring(2)
            return new Date(line.substring(0, 4), line.substring(5, 7) - 1, line.substring(8))
        }

        if (line.length > 10 && line[4] == ':' && line[7] == ':') {
            try {
                if (line.substring(0, 2) == "01")
                    line = "20" + line.substring(2)
                let hours = JSON.parse(line.substring(10))
                if (hours && hours.length == 3) {
                    return new Date(line.substring(0, 4), line.substring(5, 7) - 1, line.substring(8, 10), hours[0], hours[1], hours[2])
                }
            } catch (e) {
            }
        }

        let d = new Date(line)
        // test invalid date
        if (d != "Invalid Date") {
            return d
        }

        var dateParts = line.split(/[:\s]+/);
        if (dateParts.length == 6) {
            let dd = new Date(dateParts[0], dateParts[1] - 1, dateParts[2], dateParts[3], dateParts[4], dateParts[5])
            if (dd != "Invalid Date") {
                //console.log(`parsed ${line}: ${dd}`)
                return dd
            }
        }

        if (dateParts.length == 5) {
            let dd = new Date(dateParts[0], dateParts[1] - 1, dateParts[2], dateParts[3], dateParts[4], 0)
            if (dd != "Invalid Date") {
                //console.log(`parsed ${line}: ${dd}`)
                return dd
            }
        }

        return null
    }
    catch (e) {
        return null
    }
}

for (let line of lines) {
    try {
        let d = parseDate(line)
        if (d == null || d == "Invalid Date") {
            console.log("CANNOT PARSE " + line)
        } else {
            console.log(`${d.toISOString().slice(0, 16).replace('T', ' ')}   (${line})`)
        }
    }
    catch (e) {
        console.log("ERROR: " + line)
    }
}


console.log(lines.length)