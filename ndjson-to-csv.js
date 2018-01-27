const FS = require('fs')
const Process = require('process')
const Yargs = require('yargs')
const Progress = require('progress')
const Highland = require('highland')
const NDJson = require('ndjson')
const Flat = require('flat')
const CSVWriter = require('csv-write-stream')

async function setup() {
    const interface = Yargs
        .usage('Usage: ndjson-to-csv [filename]')
        .wrap(null)
        .help('?').alias('?', 'help')
        .version().alias('v', 'version')
    try {
        const input = interface.argv._[0]
        if (input === undefined && Process.stdin.isTTY) return interface.showHelp()
        if (input === undefined || input === '-') throw new Error('Error: reading from standard input not yet supported')
        const output = await run(input)
        output.pipe(CSVWriter()).pipe(Process.stdout)
    }
    catch (e) {
        console.error(e.message)
        Process.exit(1)
    }
}

async function run(filename) {
    console.error('Starting up...')
    const total = await length(read(filename))
    const headersData = read(filename)
    headersData.observe().each(ticker('Detecting headers', total))
    const headers = await detectHeaders(headersData)
    const bodyData = read(filename)
    bodyData.observe().each(ticker('Writing data     ', total))
    const body = processBody(bodyData, headers)
    return body
}

function ticker(text, total) {
    const progress = new Progress(text + ' [:bar] :percent / :etas left', { total, width: 50 })
    return () => progress.tick()
}

function read(filename) {
    return Highland(FS.createReadStream(filename)).through(NDJson.parse())
}

function length(input) {
    return input.reduce(0, a => a + 1).toPromise(Promise)
}

function detectHeaders(input) {
    return input
        .reduce([], (a, row) => {
            const keys = Object.keys(Flat(row))
            return Array.from(new Set(a.concat(keys)))
        })
        .toPromise(Promise)
}

function processBody(input, headers) {
    return input.map(row => {
        const rowFlat = Flat(row)
        return headers.slice().reverse().reduce((a, header) => {
            return Object.assign({ [header]: rowFlat[header] }, a)
        }, {})
    })
}

setup()
