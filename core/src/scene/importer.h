#pragma once

#include "util/url.h"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "yaml-cpp/yaml.h"
#include "scene/scene.h"

namespace Tangram {

class Platform;
class Asset;

class Importer {

public:

    using Node = YAML::Node;

    Importer(std::shared_ptr<Scene> _scene);

    // Loads the main scene with deep merging dependent imported scenes.
    Node applySceneImports(const std::shared_ptr<Platform>& platform);

// protected for testing purposes, else could be private
protected:
    virtual std::string getSceneString(const std::shared_ptr<Platform>& platform,
            const Url& scenePath);

    void processScene(const std::shared_ptr<Platform>& platform, const Url& scenePath, const std::string& sceneString);

    // Get the sequence of scene names that are designated to be imported into the
    // input scene node by its 'import' fields.
    std::vector<Url> getResolvedImportUrls(const std::shared_ptr<Platform>& platform, const Node& scene, const Url& base);

    // loads all the imported scenes and the master scene and returns a unified YAML root node.
    void importScenesRecursive(const std::shared_ptr<Platform>& platform, Node& root,
            const Url& scenePath, std::vector<Url>& sceneStack);

    void mergeMapFields(Node& target, const Node& import);

    void resolveSceneUrls(const std::shared_ptr<Platform>& platform, Node& root, const Url& base);
    void createSceneAsset(const std::shared_ptr<Platform>& platform, const Url& resolvedUrl,
            const Url& relativeUrl, const Url& base);

private:
    // import scene to respective root nodes
    std::unordered_map<Url, Node> m_scenes;

    std::vector<Url> m_sceneQueue;
    std::shared_ptr<Scene> m_scene;

    static std::atomic_uint progressCounter;
    std::mutex sceneMutex;
    std::condition_variable m_condition;

    const unsigned int MAX_SCENE_DOWNLOAD = 4;
};

}
