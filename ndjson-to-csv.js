import FS from 'fs'
import Scramjet from 'scramjet'
import NDJson from 'ndjson'
import StreamArray from 'stream-json/streamers/StreamArray.js'
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
    const stream = isArray
        ? Scramjet.DataStream.from(FS.createReadStream(filename).pipe(StreamArray.withParser())).map(entry => entry.value)
        : Scramjet.DataStream.from(FS.createReadStream(filename).pipe(NDJson.parse()))
    if (!retainPaths || retainPaths.length === 0) return stream
    return stream.map(row => {
        return retainPaths.reduce((a, path) => {
            const data = JSON.stringify(extract(a, path))
            return Object.assign(a, { [path]: data })
        }, row)
    })
}

function length(input) {
    return input.reduce(a => a + 1, 0)
}

function detectHeaders(input, useFirstRow) {
    if (useFirstRow) return input.slice(0, 1).flatMap(row => Object.keys(Flat(row))).toArray()
    return input.reduce((a, row) => {
        const keys = Object.keys(Flat(row))
        return Array.from(new Set(a.concat(keys)))
    }, [])
}

function process(input, headers) {
    return input.map(row => {
        const rowFlat = Flat(row)
        return headers.slice().reverse().reduce((a, header) => {
            return Object.assign({ [header]: rowFlat[header] }, a)
        }, {})
    })
}

async function run(filename, onlyShowHeaders = false, useFirstRowHeaders = false, isArray = false, retainPaths = [], enableLogging = true, ticker = () => {}) {
    const total = enableLogging ? await length(read(filename, isArray)) : null
    const headersData = read(filename, isArray, retainPaths)
    if (enableLogging) headersData.each(ticker('Detecting headers...', total))
    const headers = await detectHeaders(headersData, useFirstRowHeaders)
    if (onlyShowHeaders) return headers
    const bodyData = read(filename, isArray, retainPaths)
    if (enableLogging) bodyData.each(ticker('Writing data...     ', total))
    const body = process(bodyData, headers)
    return body
}

export default run
