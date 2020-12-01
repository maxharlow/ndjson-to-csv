import Process from 'process'
import Yargs from 'yargs'
import Progress from 'progress'
import run from './ndjson-to-csv.js'

function ticker(text, total) {
    const progress = new Progress(text + ' |:bar| :percent / :etas left', {
        total,
        width: Infinity,
        complete: '█',
        incomplete: ' '
    })
    return () => progress.tick()
}

async function setup() {
    const instructions = Yargs(Process.argv.slice(2))
        .usage('Usage: ndjson-to-csv <filename>')
        .wrap(null)
        .option('e', { alias: 'only-show-headers', type: 'boolean', description: 'Only list the headers from the file' })
        .option('f', { alias: 'use-first-row-headers', type: 'boolean', description: 'Use the headers from the first row (faster)' })
        .option('a', { alias: 'is-array', type: 'boolean', description: 'Input is a Json array' })
        .option('r', { alias: 'retain', type: 'array', description: 'A path under which to retain the Json structure' })
        .option('q', { alias: 'quiet', type: 'boolean', description: 'Don\'t print out progress (faster)' })
        .help('?').alias('?', 'help')
        .version().alias('v', 'version')
        .demandCommand(1, '')
    try {
        const {
            _: [input],
            onlyShowHeaders,
            useFirstRowHeaders,
            isArray,
            retain,
            quiet
        } = instructions.argv
        console.error('Starting up...')
        const output = await run(input, onlyShowHeaders, useFirstRowHeaders, isArray, retain, !quiet, quiet ? () => {} : ticker)
        if (onlyShowHeaders) output.forEach(header => console.log(header))
        else output.CSVStringify().each(console.log)
    }
    catch (e) {
        console.error(e.message)
        Process.exit(1)
    }
}

setup()
