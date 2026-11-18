"""
Frontend extension registration for ComfyUI Cluster Synchronization.
Core backend hooks are managed by app.cluster_sync.ClusterSyncManager.
"""

WEB_DIRECTORY = "./web"
NODE_CLASS_MAPPINGS = {}
NODE_DISPLAY_NAME_MAPPINGS = {}

__all__ = ["WEB_DIRECTORY", "NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
