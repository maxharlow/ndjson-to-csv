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
        .option('e', { alias: 'only-show-headers', type: 'boolean', description: 'Only list the headers from the file' })
        .option('f', { alias: 'use-first-row-headers', type: 'boolean', description: 'Use the headers from the first row (faster)' })
        .option('r', { alias: 'retain', type: 'array', description: 'A path under which to retain the Json structure' })
        .option('q', { alias: 'quiet', type: 'boolean', description: 'Don\'t print out progress (faster)' })
        .help('?').alias('?', 'help')
        .version().alias('v', 'version')
    try {
        const input = interface.argv._[0]
        if (input === undefined && Process.stdin.isTTY) return interface.showHelp()
        if (input === undefined || input === '-') throw new Error('Error: reading from standard input not yet supported')
        const output = await run(input, interface.argv.onlyShowHeaders, interface.argv.useFirstRowHeaders, interface.argv.retain, !interface.argv.quiet)
        if (interface.argv.onlyShowHeaders) output.each(console.log)
        else output.pipe(CSVWriter()).pipe(Process.stdout)
    }
    catch (e) {
        console.error(e.message)
        Process.exit(1)
    }
}

async function run(filename, onlyShowHeaders, useFirstRowHeaders, retainPaths, enableLogging) {
    if (enableLogging) console.error('Starting up...')
    const total = enableLogging ? await length(read(filename)) : null
    const headersData = read(filename, retainPaths)
    if (enableLogging) headersData.observe().each(ticker('Detecting headers', total))
    const headers = await detectHeaders(headersData, useFirstRowHeaders)
    if (onlyShowHeaders) return Highland(headers)
    const bodyData = read(filename, retainPaths)
    if (enableLogging) bodyData.observe().each(ticker('Writing data     ', total))
    const body = processBody(bodyData, headers)
    return body
}

function ticker(text, total) {
    const progress = new Progress(text + ' [:bar] :percent / :etas left', { total, width: 50 })
    return () => progress.tick()
}

function extract(object, path) {
    const paths = path.split('.')
    return paths.reduce((a, key, i) => {
        if (!a || !a[key]) return undefined
        else if (i === paths.length - 1) {
            const data = a[key]
            delete a[key]
            return data
        }
        else return a[key]
    }, object);
}

function read(filename, retainPaths) {
    return Highland(FS.createReadStream(filename))
        .through(NDJson.parse())
        .map(row => {
            if (!retainPaths) return row
            return retainPaths.reduce((a, path) => {
                const data = JSON.stringify(extract(a, path))
                return Object.assign(a, { [path]: data })
            }, row)
        })
}

function length(input) {
    return input.reduce(0, a => a + 1).toPromise(Promise)
}

function detectHeaders(input, useFirstRow) {
    if (useFirstRow) return input
        .head()
        .map(row => {
            return Object.keys(Flat(row))
        })
        .toPromise(Promise)
    else return input
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
