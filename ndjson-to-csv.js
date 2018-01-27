const FS = require('fs')
const Process = require('process')
const Yargs = require('yargs')
const Progress = require('progress')
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
        console.error('Starting up...')
        const total = await length(filename)
        const headersProgress = new Progress('Detecting headers [:bar] :percent / :etas left', { total, width: 50 })
        const headersTick = () => headersProgress.tick()
        const headers = await processHeaders(filename, headersTick)
        const bodyProgress = new Progress('Writing data      [:bar] :percent / :etas left', { total, width: 50 })
        const bodyTick = () => bodyProgress.tick()
        processBody(filename, headers, bodyTick)
            .through(CSVWriter())
            .pipe(Process.stdout)
    }
    catch (e) {
        console.error(e.message)
        Process.exit(1)
    }
}

function length(filename) {
    return Highland(FS.createReadStream(filename))
        .through(NDJson.parse())
        .reduce(0, a => a + 1)
        .toPromise(Promise)
}

function processHeaders(filename, tick) {
    return Highland(FS.createReadStream(filename))
        .through(NDJson.parse())
        .reduce([], (a, row) => {
            tick()
            const keys = Object.keys(Flat(row))
            return Array.from(new Set(a.concat(keys)))
        })
        .toPromise(Promise)
}

function processBody(filename, headers, tick) {
    return Highland(FS.createReadStream(filename))
        .through(NDJson.parse())
        .map(row => {
            tick()
            const rowFlat = Flat(row)
            return headers.slice().reverse().reduce((a, header) => {
                return Object.assign({ [header]: rowFlat[header] }, a)
            }, {})
        })
}

setup()
