#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

/*
 * AIND Ephys curation pipeline: re-applies curation to an already processed (sorted) ephys asset.
 *   per stream:  apply_curation -> quality_control (processing metrics only, no raw data)
 *   per session: quality_control_collector (keeps the raw-data QC of the processed asset)
 *                job_dispatch -> nwb_units (replaces the units table of the processed asset NWB)
 * Inputs: --ecephys_url (raw session) and --sorted_url (processed asset), local paths or s3:// URLs.
 * Outputs (RESULTS_PATH): postprocessed/, curated/, quality_control.json, quality_control/, nwb/
 */

params.ecephys_url = null
params.sorted_url = null
params.params_file = null
params.extra_params = null
params.extra_installs = ""
params.executor = "local"
params.n_jobs = null
params.container_tag = System.getenv('CONTAINER_TAG') ?: '1.5.1'

if (!params.ecephys_url || !params.sorted_url) {
    error "Both --ecephys_url (raw session) and --sorted_url (processed asset) are required"
}
ecephys_path = params.ecephys_url.toString().replaceAll('/+$', '')
sorted_path = params.sorted_url.toString().replaceAll('/+$', '')
println "ECEPHYS (raw session): ${ecephys_path}"
println "SORTED (processed asset): ${sorted_path}"
println "RESULTS_PATH: ${RESULTS_PATH}"
println "CONTAINER TAG: ${params.container_tag}"

gitCloneFunction = '''
clone_repo() {
    local repo_url="$1"
    local commit_hash="$2"
    echo "cloning git repo: ${repo_url} (commit: ${commit_hash})..."
    git clone "${repo_url}" capsule-repo
    git -C capsule-repo -c core.fileMode=false checkout "${commit_hash}" --quiet
    mv capsule-repo/code capsule/code
    rm -rf capsule-repo
}
'''

// Recursively merge `over` into `base` (returns a new map)
def deepMerge(Map base, Map over) {
    def out = new LinkedHashMap(base)
    over.each { k, v ->
        out[k] = (out[k] instanceof Map && v instanceof Map) ? deepMerge(out[k], v) : v
    }
    return out
}

// Parameters: params file (or default_params.json), then dotted CLI overrides, e.g. --apply_curation.merging_mode hard
def params_file = params.params_file ?: "${projectDir}/default_params.json"
def full_params = new groovy.json.JsonSlurper().parseText(file(params_file).text)
println "Loaded parameters from ${params_file}"
def step_keys = ['logging', 'job_dispatch', 'apply_curation', 'quality_control', 'quality_control_collector', 'nwb_units']
// Same overrides packed in one string, for Code Ocean (only App Panel params are forwarded), e.g.
// --extra_params "--apply_curation.merging_mode=hard --apply_curation.censor_ms=0.3"
// shortcut: whitespace split, so values can't contain spaces; use the params file for those
def parseValue = { String v -> v == 'true' ? true : v == 'false' ? false : v.isNumber() ? v.toBigDecimal() : v }
def cli_overrides = [:]
if (params.extra_params instanceof String) {
    def tokens = params.extra_params.trim().split(/\s+/) as List
    for (int i = 0; i < tokens.size(); i++) {
        if (!tokens[i].startsWith('--')) continue
        def (key, value) = tokens[i].substring(2).contains('=') ? tokens[i].substring(2).split('=', 2) as List :
            [tokens[i].substring(2), (i + 1 < tokens.size() && !tokens[i + 1].startsWith('--')) ? tokens[++i] : 'true']
        def path = key.split(/\./) as List
        if (!(path[0] in step_keys) || path.size() < 2) {
            println "WARN: ignoring extra_params entry '--${key}' (must start with one of ${step_keys})"
            continue
        }
        cli_overrides = deepMerge(cli_overrides, path.reverse().inject(parseValue(value)) { acc, k -> [(k): acc] })
    }
}
cli_overrides = deepMerge(cli_overrides, step_keys.findAll { params.containsKey(it) && params[it] instanceof Map }.collectEntries { [(it): params[it]] })
if (cli_overrides) {
    println "Merging CLI parameters: ${cli_overrides}"
    full_params = deepMerge(full_params, cli_overrides)
}
step_keys.each { full_params[it] = full_params[it] ?: [:] }

// Streaming: job_dispatch reads the raw data from the remote session and only its metadata is staged
streaming = full_params.job_dispatch.streaming ?: false
if (streaming && !(ecephys_path ==~ /^\w+:\/\/.+/)) {
    println "WARN: job_dispatch.streaming is set but ${ecephys_path} is local: staging it as usual"
    streaming = false
}
if (streaming && full_params.job_dispatch.input != 'aind') {
    println "WARN: job_dispatch.streaming is only supported with input 'aind': staging the full session"
    streaming = false
}
if (streaming) {
    full_params.job_dispatch.remote_path = full_params.job_dispatch.remote_path ?: ecephys_path
}
println "STREAMING: ${streaming}"

// Tag logs with the acquisition name of the raw session
if (!full_params.logging.acquisition_name) {
    def acquisition_name = 'undefined'
    try {
        def data_description = file("${ecephys_path}/data_description.json")
        if (data_description.exists()) {
            acquisition_name = new groovy.json.JsonSlurper().parseText(data_description.text).name ?: acquisition_name
        }
    } catch (Exception e) {
        println "WARN: could not read data_description.json: ${e.message}"
    }
    full_params.logging.acquisition_name = acquisition_name
}
full_params.logging.pipeline_name = full_params.logging.pipeline_name ?: 'AIND Ephys Curation Pipeline'

// Staged into every task as capsule/params.json
full_params_file = new File("${workDir}/full_params.json")
full_params_file.parentFile.mkdirs()
// Rewrite only on change: tasks hash the staged file's mtime, so touching it would invalidate -resume
def full_params_text = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(full_params))
if (!full_params_file.exists() || full_params_file.text != full_params_text) {
    full_params_file.text = full_params_text
}
println "Full parameters written to ${full_params_file}"

// Step repos and commits: process_versions_custom.env (if present) or process_versions.env,
// overridable per step with --<step>_repo / --<step>_version
versions = [:]
def versions_file = file("${projectDir}/process_versions_custom.env").exists() ?
    file("${projectDir}/process_versions_custom.env") : file("${projectDir}/process_versions.env")
versions_file.eachLine { line ->
    if (line.contains('=') && !line.trim().startsWith('#')) {
        def idx = line.indexOf('=')
        versions[line.substring(0, idx).trim()] = line.substring(idx + 1).trim().replaceAll(/^["']|["']$/, '')
    }
}
['job_dispatch', 'apply_curation', 'quality_control', 'quality_control_collector', 'nwb_units'].each { step ->
    def key = step.toUpperCase()
    if (params.containsKey("${step}_repo")) { versions["${key}_REPO"] = params["${step}_repo"] }
    if (params.containsKey("${step}_version")) { versions["${key}_VERSION"] = params["${step}_version"] }
    println "${key}: ${versions["${key}_REPO"]} @ ${versions["${key}_VERSION"]}"
}

def extra_installs_list = params.extra_installs ? params.extra_installs.split(',').collect { it.trim() }.findAll { it } : []
extra_installs_cmd = extra_installs_list ? "pip install " + extra_installs_list.collect { "'" + it + "'" }.join(' ') : ""

// Clones the step repo at its pinned commit into capsule/code and runs it
def runStep(String key, cpus) {
    """
    ${extra_installs_cmd}
    export CODE_REPO="${versions[key + '_REPO']}"
    export CODE_VERSION="${versions[key + '_VERSION']}"
    export N_JOBS_EXT=${params.get('n_jobs') ?: cpus}
    mkdir -p capsule/data capsule/results capsule/scratch
    ${gitCloneFunction}
    clone_repo "\$CODE_REPO" "\$CODE_VERSION"
    cd capsule/code
    chmod +x run
    ./run
    """
}

// Published layout of the curated asset: same as the processed asset
def curatedAssetPath(String filename) {
    def name = new File(filename).getName()
    def analyzer = name =~ /^postprocessed_(.+)\.zarr$/
    if (analyzer.matches()) return "postprocessed/${analyzer.group(1)}.zarr"
    def curation = name =~ /^curation_(.+)\.json$/
    if (curation.matches()) return "curated/${curation.group(1)}/curation.json"
    return null
}

// Streaming (AIND input): copy only the small metadata of a remote session into metadata_folder
// (same as aind-ephys-pipeline: session JSONs, behavior JSONs + raw.harp placeholder, small ecephys_clipped files)
def aind_partial_staging(session_path, metadata_folder) {
    def session_root = file(session_path)
    def behavior_jsons = files("${session_path}/behavior/**.json")
    def clipped_patterns = ['**.{csv,xml,oebin}', '**/sync_messages.txt', '**/events/**']
    def metadata_files = files("${session_path}/*.json") + behavior_jsons +
        ['ecephys_clipped', 'ecephys/ecephys_clipped'].collectMany { clipped ->
            clipped_patterns.collectMany { files("${session_path}/${clipped}/${it}") }
        }
    metadata_folder.mkdirs()
    metadata_files.each { f ->
        def target = metadata_folder.resolve(session_root.relativize(f).toString())
        // shortcut: copy only if missing, to keep mtimes stable for -resume; a changed remote file needs a clean workDir
        if (!target.exists()) { target.parent.mkdirs(); f.copyTo(target) }
    }
    behavior_jsons*.parent.unique().findAll { it.resolve('raw.harp').exists() }.each { d ->
        def marker = metadata_folder.resolve(session_root.relativize(d).toString()).resolve('raw.harp/PLACEHOLDER')
        if (!marker.exists()) { marker.parent.mkdirs(); marker.text = 'HARP data is not staged in streaming mode\n' }
    }
    return metadata_folder
}

process job_dispatch {
    tag 'job-dispatch'
    container "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:${params.container_tag}"

    input:
    path ecephys_session, stageAs: 'capsule/data/ecephys_session'
    path params_file, stageAs: 'capsule/params.json'

    output:
    path 'capsule/results/job_*.json', emit: jobs

    script:
    """
    #!/usr/bin/env bash
    set -e
    ${runStep('JOB_DISPATCH', task.cpus)}
    """
}

process apply_curation {
    tag "apply-curation ${meta.id}"
    container "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:${params.container_tag}"
    publishDir "$RESULTS_PATH", mode: 'copy', saveAs: { filename -> curatedAssetPath(filename) }

    input:
    tuple val(meta), path(analyzer, stageAs: 'capsule/data/postprocessed/*'), path(curation_json, stageAs: 'capsule/data/curation_input.json')
    path params_file, stageAs: 'capsule/params.json'

    output:
    tuple val(meta), path('capsule/results/*', arity: '1..*'), emit: results

    script:
    """
    #!/usr/bin/env bash
    set -e
    # the step reads the processed asset layout: curated/{recording_name}/curation.json
    if [ -e capsule/data/curation_input.json ]; then
        mkdir -p "capsule/data/curated/${meta.id}"
        ln -s ../../curation_input.json "capsule/data/curated/${meta.id}/curation.json"
    fi
    ${runStep('APPLY_CURATION', task.cpus)}
    """
}

process quality_control {
    tag "quality-control ${meta.id}"
    container "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:${params.container_tag}"

    input:
    tuple val(meta), path(curation_results, stageAs: 'capsule/data/*')
    path params_file, stageAs: 'capsule/params.json'

    output:
    path 'capsule/results/*', emit: results

    script:
    """
    #!/usr/bin/env bash
    set -e
    ${runStep('QUALITY_CONTROL', task.cpus)}
    """
}

process quality_control_collector {
    tag 'qc-collector'
    container "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-base:${params.container_tag}"
    publishDir "$RESULTS_PATH", mode: 'copy', saveAs: { filename -> new File(filename).getName() }

    input:
    path quality_control_results, stageAs: 'capsule/data/*'
    path previous_qc, stageAs: 'capsule/data/previous_qc/*'
    path params_file, stageAs: 'capsule/params.json'

    output:
    path 'capsule/results/*'

    script:
    """
    #!/usr/bin/env bash
    set -e
    ${runStep('QUALITY_CONTROL_COLLECTOR', task.cpus)}
    """
}

process nwb_units {
    tag 'nwb-units'
    container "ghcr.io/allenneuraldynamics/aind-ephys-pipeline-nwb:${params.container_tag}"
    publishDir "$RESULTS_PATH/nwb", mode: 'copy', saveAs: { filename -> new File(filename).getName() }

    input:
    path ecephys_session, stageAs: 'capsule/data/ecephys_session'
    path job_jsons, stageAs: 'capsule/data/*'
    path analyzers, stageAs: 'capsule/data/*'
    path spikesorted, stageAs: 'capsule/data/spikesorted'
    path nwb_files, stageAs: 'capsule/data/*'
    path params_file, stageAs: 'capsule/params.json'

    output:
    path 'capsule/results/*'

    script:
    """
    #!/usr/bin/env bash
    set -e
    # the step reads the processed asset layout: postprocessed/{recording_name}.zarr
    mkdir -p capsule/data/postprocessed
    for analyzer in capsule/data/postprocessed_*.zarr; do
        name=\$(basename "\$analyzer")
        ln -s "../\$name" "capsule/data/postprocessed/\${name#postprocessed_}"
    done
    ${runStep('NWB_UNITS', task.cpus)}
    """
}

workflow {
    def params_ch = Channel.value(file(full_params_file.toString()))
    def session_path = streaming ? aind_partial_staging(ecephys_path, file("${workDir}/streaming_session")).toString() : ecephys_path
    def session_ch = Channel.fromPath(session_path + "/", type: 'any').collect()

    // one stream per analyzer of the processed asset, with its curation.json if there is one
    def streams = Channel.fromPath("${sorted_path}/postprocessed/*.zarr", type: 'dir').map { analyzer ->
        def recording_name = analyzer.name - ~/\.zarr$/
        def curation_json = file("${sorted_path}/curated/${recording_name}/curation.json")
        [[id: recording_name], analyzer, curation_json.exists() ? curation_json : []]
    }

    apply_curation_out = apply_curation(streams, params_ch)
    quality_control_out = quality_control(apply_curation_out.results, params_ch)

    def previous_qc = [file("${sorted_path}/quality_control.json"), file("${sorted_path}/quality_control")].findAll { it.exists() }
    quality_control_collector(quality_control_out.results.collect(), Channel.value(previous_qc), params_ch)

    job_dispatch_out = job_dispatch(session_ch, params_ch)
    def spikesorted = file("${sorted_path}/spikesorted")
    nwb_units(
        session_ch,
        job_dispatch_out.jobs.collect(),
        apply_curation_out.results.map { meta, files -> files.findAll { it.name.startsWith('postprocessed_') } }.flatten().collect(),
        Channel.value(spikesorted.exists() ? spikesorted : []),
        Channel.fromPath("${sorted_path}/nwb/*.nwb*", type: 'any').collect(),
        params_ch
    )
}
