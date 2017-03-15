#include "scene/importer.h"

#include "log.h"
#include "platform.h"
#include "scene/sceneLoader.h"

#include <regex>
#include "yaml-cpp/yaml.h"

using YAML::Node;
using YAML::NodeType;

namespace Tangram {

std::atomic_uint Importer::progressCounter(0);

Importer::Importer(std::shared_ptr<Scene> scene) : m_scene(scene) {}

Url getBundledPath(const Url& url) {

    auto rootScene = url.string();
    const char* yamlExt = ".yaml";
    const char* extStr = ".zip";

    auto extLoc = rootScene.rfind(extStr);
    if (extLoc == std::string::npos) { return url; }

    auto loc = rootScene.rfind("/");
    if (extLoc == std::string::npos) { return url; }

    return Url(rootScene.substr(loc, extLoc - loc) + yamlExt);
}

Node Importer::applySceneImports(const std::shared_ptr<Platform>& platform) {

    const Url& scenePath = m_scene->path();
    const Url& resourceRoot = m_scene->resourceRoot();

    Url path;
    Url sPath;
    bool isZipped = false;
    Url bundledScenePath; //relative of the root scene inside the zip bundle
    Url rootScenePath = scenePath.resolved(resourceRoot);

    m_sceneQueue.push_back(rootScenePath);


    while (true) {
        {
            std::unique_lock<std::mutex> lock(sceneMutex);

            m_condition.wait(lock, [&, this]{
                    if (m_sceneQueue.empty()) {
                        // Not busy at all?
                        if (progressCounter == 0) { return true; }
                    } else {
                        // More work and not completely busy?
                        if (progressCounter < MAX_SCENE_DOWNLOAD) { return true; }
                    }

                    return false;
                });


            if (m_sceneQueue.empty()) {
                if (progressCounter == 0) {
                    break;
                }
                continue;
            }

            path = m_sceneQueue.back();
            sPath = path;
            m_sceneQueue.pop_back();

            const char* extStr = ".zip";
            const size_t extLength = strlen(extStr);
            const size_t urlLength = path.string().length();
            if (urlLength > extLength && (path.string().compare(urlLength - extLength, extLength, extStr) == 0)) {
                bundledScenePath = getBundledPath(path);
                auto str = sPath.string();
                sPath = Url(str.replace(urlLength - extLength, extLength, "/" + bundledScenePath.string()));
                isZipped = true;
            }

            if (m_scenes.find(sPath) != m_scenes.end()) { continue; }
        }



        if (path.hasHttpScheme()) {
            // TODO: more required here
            progressCounter++;
            platform->startUrlRequest(path.string(), [&, isZipped, path](std::vector<char>&& rawData) {
                if (!rawData.empty()) {
                    std::unique_lock<std::mutex> lock(sceneMutex);
                    if (isZipped) {
                        createSceneAsset(sPath, bundledScenePath, Url(""), rawData);
                    } else {
                        createSceneAsset(sPath, bundledScenePath, Url(""));
                    }
                    processScene(path, std::string(rawData.data(), rawData.size()));
                }
                progressCounter--;
                m_condition.notify_all();
            });
        } else {
            std::unique_lock<std::mutex> lock(sceneMutex);
            if (isZipped) {
                auto& asset = createSceneAsset(sPath, bundledScenePath, Url(""), platform->bytesFromFile(path.string().c_str()));
                processScene(sPath, asset->readStringFromAsset(platform));
            } else {
                auto& asset = createSceneAsset(sPath, bundledScenePath, Url(""));
                processScene(sPath, asset->readStringFromAsset(platform));
            }
        }
    }

    Node root = Node();

    LOGD("Processing scene import Stack:");
    std::vector<Url> sceneStack;
    importScenesRecursive(platform, root, rootScenePath, sceneStack);

    return root;
}

void Importer::processScene(const Url& scenePath, const std::string &sceneString) {

    LOGD("Process: '%s'", scenePath.string().c_str());

    // Don't load imports twice
    if (m_scenes.find(scenePath) != m_scenes.end()) {
        return;
    }

    try {
        auto sceneNode = YAML::Load(sceneString);

        m_scenes[scenePath] = sceneNode;

        for (const auto& import : getResolvedImportUrls(sceneNode, scenePath)) {
            m_sceneQueue.push_back(import);
            m_condition.notify_all();
        }
    } catch (YAML::ParserException e) {
        LOGE("Parsing scene config '%s'", e.what());
    }
}

bool nodeIsPotentialUrl(const Node& node) {
    // Check that the node is scalar and not null.
    if (!node || !node.IsScalar()) { return false; }

    // Check that the node does not contain a 'global' reference.
    if (node.Scalar().compare(0, 7, "global.") == 0) { return false; }

    return true;
}

bool nodeIsTextureUrl(const Node& node, const Node& textures) {

    if (!nodeIsPotentialUrl(node)) { return false; }

    // Check that the node is not a number or a boolean.
    bool booleanValue = false;
    double numberValue = 0.;
    if (YAML::convert<bool>::decode(node, booleanValue)) { return false; }
    if (YAML::convert<double>::decode(node, numberValue)) { return false; }

    // Check that the node does not name a scene texture.
    if (textures[node.Scalar()]) { return false; }

    return true;
}

const std::unique_ptr<Asset>& Importer::createSceneAsset(const Url& resolvedUrl, const Url& relativeUrl, const Url& base,
        const std::vector<char>& zipData) {

    auto& sceneAssets = m_scene->sceneAssets();
    auto& resolvedStr = resolvedUrl.string();
    auto& relativeStr = relativeUrl.string();
    auto& baseStr = base.string();

    if (sceneAssets.find(resolvedStr) != sceneAssets.end()) {
        return sceneAssets[resolvedStr];
    }

    if (base.isEmpty()) {
        // Build asset for root scene (does not have any base)
        sceneAssets[resolvedStr] = std::make_unique<Asset>(resolvedStr, relativeStr, zipData);
    } else if (relativeUrl.isAbsolute()) {
        // If relativeUrl is absolute then ignore parent's (base's zipHandle) (this asset is not
        // part of the zip bundle)
        sceneAssets[resolvedStr] = std::make_unique<Asset>(resolvedStr, relativeStr);
    } else {
        assert(zipData.empty());
        auto zipHandle = sceneAssets[baseStr]->zipHandle();
        sceneAssets[resolvedStr] = std::make_unique<Asset>(resolvedStr, relativeStr, zipData, zipHandle);
    }

    return sceneAssets[resolvedStr];
}

void Importer::resolveSceneUrls(const std::shared_ptr<Platform>& platform, Node& root,
        const Url& base) {

    // Resolve global texture URLs.
    std::string relativeUrl = "";

    Node textures = root["textures"];

    if (textures) {
        for (auto texture : textures) {
            if (Node textureUrlNode = texture.second["url"]) {
                if (nodeIsPotentialUrl(textureUrlNode)) {
                    relativeUrl = textureUrlNode.Scalar();
                    textureUrlNode = Url(textureUrlNode.Scalar()).resolved(base).string();
                    createSceneAsset(textureUrlNode.Scalar(), relativeUrl, base);
                }
            }
        }
    }

    // Resolve inline texture URLs.

    if (Node styles = root["styles"]) {

        for (auto entry : styles) {

            Node style = entry.second;
            if (!style.IsMap()) { continue; }

            //style->texture
            if (Node texture = style["texture"]) {
                if (nodeIsTextureUrl(texture, textures)) {
                    relativeUrl = texture.Scalar();
                    texture = Url(texture.Scalar()).resolved(base).string();
                    createSceneAsset(texture.Scalar(), relativeUrl, base);
                }
            }

            //style->material->texture
            if (Node material = style["material"]) {
                if (!material.IsMap()) { continue; }
                for (auto& prop : {"emission", "ambient", "diffuse", "specular", "normal"}) {
                    if (Node propNode = material[prop]) {
                        if (!propNode.IsMap()) { continue; }
                        if (Node matTexture = propNode["texture"]) {
                            if (nodeIsTextureUrl(matTexture, textures)) {
                                relativeUrl = matTexture.Scalar();
                                matTexture = Url(matTexture.Scalar()).resolved(base).string();
                                createSceneAsset(matTexture.Scalar(), relativeUrl, base);
                            }
                        }
                    }
                }
            }

            //style->shader->uniforms->texture
            if (Node shaders = style["shaders"]) {
                if (!shaders.IsMap()) { continue; }
                if (Node uniforms = shaders["uniforms"]) {
                    for (auto uniformEntry : uniforms) {
                        Node uniformValue = uniformEntry.second;
                        if (nodeIsTextureUrl(uniformValue, textures)) {
                            relativeUrl = uniformValue.Scalar();
                            uniformValue = Url(uniformValue.Scalar()).resolved(base).string();
                            createSceneAsset(uniformValue.Scalar(), relativeUrl, base);
                        } else if (uniformValue.IsSequence()) {
                            for (Node u : uniformValue) {
                                if (nodeIsTextureUrl(u, textures)) {
                                    relativeUrl = u.Scalar();
                                    u = Url(u.Scalar()).resolved(base).string();
                                    createSceneAsset(u.Scalar(), relativeUrl, base);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Resolve data source URLs.

    // TODO/NOTE/DISCUSS: can mbtiles or other datasource be inside a zip bundle.. can we handle it
    // normally?
    if (Node sources = root["sources"]) {
        for (auto source : sources) {
            if (!source.second.IsMap()) { continue; }
            if (Node sourceUrl = source.second["url"]) {
                if (nodeIsPotentialUrl(sourceUrl)) {
                    auto resolvedUrl = Url(sourceUrl.Scalar()).resolved(base);
                    sourceUrl = (resolvedUrl.isAbsolute()) ?
                            resolvedUrl.string() : platform->resolveAssetPath(resolvedUrl.string());
                }
            }
        }
    }

    // Resolve font URLs.

    if (Node fonts = root["fonts"]) {
        if (fonts.IsMap()) {
            for (const auto& font : fonts) {
                if (font.second.IsMap()) {
                    auto urlNode = font.second["url"];
                    if (nodeIsPotentialUrl(urlNode)) {
                        relativeUrl = urlNode.Scalar();
                        urlNode = Url(urlNode.Scalar()).resolved(base).string();
                        createSceneAsset(urlNode.Scalar(), relativeUrl, base);
                    }
                } else if (font.second.IsSequence()) {
                    for (auto& fontNode : font.second) {
                        auto urlNode = fontNode["url"];
                        if (nodeIsPotentialUrl(urlNode)) {
                            relativeUrl = urlNode.Scalar();
                            urlNode = Url(urlNode.Scalar()).resolved(base).string();
                            createSceneAsset(urlNode.Scalar(), relativeUrl, base);
                        }
                    }
                }
            }
        }
    }
}

std::string Importer::getSceneString(const std::shared_ptr<Platform>& platform,
        const Url& scenePath) {
    return platform->stringFromFile(scenePath.string().c_str());
}

std::vector<Url> Importer::getResolvedImportUrls(const Node& scene, const Url& base) {

    std::vector<Url> scenePaths;

    if (const Node& import = scene["import"]) {
        if (import.IsScalar()) {
            auto resolvedUrl = Url(import.Scalar()).resolved(base);
            createSceneAsset(resolvedUrl, import.Scalar(), base);
            scenePaths.push_back(Url(import.Scalar()).resolved(base));
        } else if (import.IsSequence()) {
            for (const auto& path : import) {
                if (path.IsScalar()) {
                    auto resolvedUrl = Url(path.Scalar()).resolved(base);
                    createSceneAsset(resolvedUrl, path.Scalar(), base);
                    scenePaths.push_back(resolvedUrl);
                }
            }
        }
    }

    return scenePaths;
}

void Importer::importScenesRecursive(const std::shared_ptr<Platform>& platform, Node& root,
        const Url& scenePath, std::vector<Url>& sceneStack) {

    LOGD("Starting importing Scene: %s", scenePath.string().c_str());

    for (const auto& s : sceneStack) {
        if (scenePath == s) {
            LOGE("%s will cause a cyclic import. Stopping this scene from being imported",
                    scenePath.string().c_str());
            return;
        }
    }

    sceneStack.push_back(scenePath);

    auto sceneNode = m_scenes[scenePath];

    if (sceneNode.IsNull()) { return; }
    if (!sceneNode.IsMap()) { return; }

    auto imports = getResolvedImportUrls(sceneNode, scenePath);

    // Don't want to merge imports, so remove them here.
    sceneNode.remove("import");

    for (const auto& url : imports) {

        importScenesRecursive(platform, root, url, sceneStack);

    }

    sceneStack.pop_back();

    mergeMapFields(root, sceneNode);

    resolveSceneUrls(platform, root, scenePath);
}

void Importer::mergeMapFields(Node& target, const Node& import) {

    for (const auto& entry : import) {

        const auto& key = entry.first.Scalar();
        const auto& source = entry.second;
        auto dest = target[key];

        if (!dest) {
            dest = source;
            continue;
        }

        if (dest.Type() != source.Type()) {
            LOGN("Merging different node types: '%s'\n'%s'\n<==\n'%s'",
                 key.c_str(), Dump(dest).c_str(), Dump(source).c_str());
        }

        switch(dest.Type()) {
            case NodeType::Scalar:
            case NodeType::Sequence:
                dest = source;
                break;

            case NodeType::Map: {
                auto newTarget = dest;
                if (source.IsMap()) {
                    mergeMapFields(newTarget, source);
                } else {
                    dest = source;
                }
                break;
            }
            default:
                break;
        }
    }
}

}
