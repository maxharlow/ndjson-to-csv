import FS from 'fs'
import Highland from 'highland'
import NDJson from 'ndjson'
const { default: StreamArray } = await import('stream-json/streamers/StreamArray.js')
import Flat from 'flat'

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

function read(filename, isArray, retainPaths) {
    const parser = isArray ? StreamArray.withParser() : NDJson.parse()
    return Highland(FS.createReadStream(filename))
        .through(parser)
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

async function run(filename, onlyShowHeaders, useFirstRowHeaders, isArray, retainPaths, enableLogging, alert, ticker) {
    if (enableLogging) alert('starting up...')
    const total = enableLogging ? await length(read(filename, isArray)) : null
    const headersData = read(filename, isArray, retainPaths)
    if (enableLogging) headersData.observe().each(ticker('detecting headers', total))
    const headers = await detectHeaders(headersData, useFirstRowHeaders)
    if (onlyShowHeaders) return Highland(headers)
    const bodyData = read(filename, isArray, retainPaths)
    if (enableLogging) bodyData.observe().each(ticker('writing data     ', total))
    const body = processBody(bodyData, headers)
    return body
}

export default run
