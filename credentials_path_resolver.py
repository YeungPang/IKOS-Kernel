"""
Credential Path Resolution Utility

Handles GOOGLE_APPLICATION_CREDENTIALS path resolution with support for:
- Relative paths (project-root relative, resolved at runtime)
- Full paths (absolute, used as-is)
- Priority: Full path takes precedence if both are set
"""

import os
from pathlib import Path
from typing import Optional


def resolve_google_credentials_path(project_root: Optional[str] = None) -> str:
    """
    Resolve GOOGLE_APPLICATION_CREDENTIALS path from environment variables.
    
    Priority order:
    1. GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH (if set and file exists)
    2. GOOGLE_APPLICATION_CREDENTIALS_RELATIVE_PATH (if set, resolved relative to project_root)
    
    Args:
        project_root: Project root directory for resolving relative paths.
                     If None, uses current working directory or tries to detect.
    
    Returns:
        Full path to credentials file
        
    Raises:
        ValueError: If no credentials path is configured or file not found
        FileNotFoundError: If resolved path does not exist
    
    Example:
        # In .env:
        # GOOGLE_APPLICATION_CREDENTIALS_RELATIVE_PATH=secr/idms-prot-key.json
        # OR
        # GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH=/absolute/path/to/credentials.json
        
        from utils.credentials import resolve_google_credentials_path
        creds_path = resolve_google_credentials_path()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    """
    
    full_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH", "").strip()
    relative_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_RELATIVE_PATH", "").strip()
    
    # Determine project root if not provided
    if project_root is None:
        # Try to find project root by looking for IDMS-Proto directory or .env file
        current = Path.cwd()
        
        # Check if IDMS-Proto is in current path
        if "IDMS-Proto" in str(current):
            # We're inside IDMS-Proto, go to its root
            project_root = str(current)
            for parent in current.parents:
                if parent.name == "IDMS-Proto":
                    project_root = str(parent)
                    break
        else:
            # Try to find IDMS-Proto directory
            search_limit = 5  # Search up to 5 levels
            temp = current
            for _ in range(search_limit):
                if (temp / "IDMS-Proto").exists():
                    project_root = str(temp / "IDMS-Proto")
                    break
                temp = temp.parent
            else:
                # Default to current directory
                project_root = str(current)
    
    project_root = Path(project_root)
    
    # Resolve full path first (takes priority)
    if full_path:
        full_path_obj = Path(full_path)
        if full_path_obj.exists():
            return str(full_path_obj.resolve())
        else:
            raise FileNotFoundError(
                f"GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH points to non-existent file: {full_path}"
            )
    
    # Resolve relative path
    if relative_path:
        resolved = project_root / relative_path
        if resolved.exists():
            return str(resolved.resolve())
        else:
            raise FileNotFoundError(
                f"GOOGLE_APPLICATION_CREDENTIALS_RELATIVE_PATH resolves to non-existent file: "
                f"{relative_path} (full path: {resolved})"
            )
    
    # No credentials path configured
    raise ValueError(
        "No GOOGLE_APPLICATION_CREDENTIALS configured. "
        "Set either GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH or GOOGLE_APPLICATION_CREDENTIALS_RELATIVE_PATH in .env"
    )


def setup_google_credentials(project_root: Optional[str] = None) -> str:
    """
    Setup GOOGLE_APPLICATION_CREDENTIALS environment variable.
    
    Resolves credentials path and sets it in os.environ.
    
    Args:
        project_root: Project root for relative path resolution
    
    Returns:
        Path that was set
    
    Raises:
        ValueError: If no credentials path is configured
        FileNotFoundError: If resolved path does not exist
    
    Example:
        import credentials
        creds_path = credentials.setup_google_credentials()
        print(f"Credentials set to: {creds_path}")
    """
    creds_path = resolve_google_credentials_path(project_root)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    return creds_path


def get_credentials_path_info() -> dict:
    """
    Get information about currently configured credentials paths.
    
    Returns:
        dict with configuration and resolved path info
    
    Example:
        info = credentials.get_credentials_path_info()
        print(f"Using: {info['source']} -> {info['resolved_path']}")
    """
    full_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_FULL_PATH", "").strip()
    relative_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_RELATIVE_PATH", "").strip()
    
    try:
        resolved = resolve_google_credentials_path()
        source = "full_path" if full_path else "relative_path"
        exists = True
    except (ValueError, FileNotFoundError) as e:
        resolved = None
        source = "error"
        exists = False
    
    return {
        "full_path_configured": bool(full_path),
        "full_path_value": full_path,
        "relative_path_configured": bool(relative_path),
        "relative_path_value": relative_path,
        "priority_source": source,
        "resolved_path": resolved,
        "file_exists": exists,
        "current_env": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
    }


if __name__ == "__main__":
    import json
    
    print("=" * 80)
    print("GOOGLE_APPLICATION_CREDENTIALS Configuration")
    print("=" * 80)
    
    info = get_credentials_path_info()
    print(json.dumps(info, indent=2))
    
    print("\nResolution Summary:")
    if info["file_exists"]:
        print(f"✓ Using {info['priority_source']}: {info['resolved_path']}")
    else:
        print("✗ No valid credentials path found")
        if info["full_path_configured"]:
            print(f"  Full path configured but file not found: {info['full_path_value']}")
        if info["relative_path_configured"]:
            print(f"  Relative path configured but file not found: {info['relative_path_value']}")
