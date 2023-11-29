import Process from 'process'
import Yargs from 'yargs'
import run from './ndjson-to-csv.js'
import cliRenderer from './cli-renderer.js'

async function setup() {
    const instructions = Yargs(Process.argv.slice(2))
        .usage('Usage: ndjson-to-csv <filename>')
        .wrap(null)
        .option('e', { alias: 'only-show-headers', type: 'boolean', description: 'Only list the headers from the file', default: false })
        .option('f', { alias: 'use-first-row-headers', type: 'boolean', description: 'Use the headers from the first row (faster)', default: false })
        .option('a', { alias: 'is-array', type: 'boolean', description: 'Input is a Json array', default: false })
        .option('r', { alias: 'retain', type: 'array', description: 'A path under which to retain the Json structure' })
        .option('R', { alias: 'retain-arrays', type: 'boolean', description: 'Retain the Json structure of all arrays' })
        .option('q', { alias: 'quiet', type: 'boolean', description: 'Don\'t print out progress (faster)', default: false })
        .help('?').alias('?', 'help')
        .version().alias('v', 'version')
        .demandCommand(1, '')
    const { alert, progress, finalise } = cliRenderer(instructions.argv.quiet)
    try {
        const {
            _: [input],
            onlyShowHeaders,
            useFirstRowHeaders,
            isArray,
            retain,
            retainArrays,
            quiet
        } = instructions.argv
        alert({
            message: 'Starting up...',
            importance: 'info'
        })
        const output = await run(input, onlyShowHeaders, useFirstRowHeaders, isArray, retain, retainArrays, quiet ? null : progress)
        if (onlyShowHeaders) output.forEach(header => console.log(header))
        else await output.CSVStringify().each(console.log).whenEnd()
        await finalise('complete')
    }
    catch (e) {
        alert({
            message: e.message,
            importance: 'error'
        })
        await finalise('error')
        Process.exit(1)
    }
}

setup()
