const FS = require('fs')
const Process = require('process')
const Yargs = require('yargs')
const Highland = require('highland')
const NDJson = require('ndjson')
const Flat = require('flat')
const CSVWriter = require('csv-write-stream')

function setup() {
    const interface = Yargs
        .usage('Usage: ndjson-to-csv [filename]')
        .wrap(null)
        .demandCommand(1, '')
        .help('?').alias('?', 'help')
        .version().alias('v', 'version')
    run(interface.argv._[0])
}

async function run(input) {
    const filename = input === undefined || input === '-' ? '/dev/stdin' : input
    try {
        if (Process.stdin.isTTY === true && filename === '/dev/stdin') throw new Error('Error: no input')
        console.error('Detecting headers...')
        const headers = await processHeaders(filename)
        console.error('Writing data...')
        processBody(filename, headers)
            .through(CSVWriter())
            .pipe(Process.stdout)
    }
    catch (e) {
        console.error(e.message)
        Process.exit(1)
    }
}

function processHeaders(filename) {
    return Highland(FS.createReadStream(filename))
        .through(NDJson.parse())
        .reduce([], (a, row) => {
            const keys = Object.keys(Flat(row))
            return Array.from(new Set(a.concat(keys)))
        })
        .toPromise(Promise)
}

function processBody(filename, headers) {
    return Highland(FS.createReadStream(filename))
        .through(NDJson.parse())
        .map(row => {
            const blank = headers.reverse().reduce((a, header) => Object.assign({ [header]: '' }, a), {})
            return Object.assign(blank, Flat(row))
        })
}

setup()
