#!/usr/bin/env python3
"""
Setup Script for ETL Data Connector
Author: Shankari S R - 3122225001125
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def print_banner():
    """Print setup banner"""
    print("=" * 60)
    print("ETL Data Connector - Setup Script")
    print("Kyureeus EdTech Program - SSN CSE")
    print("=" * 60)
    print()

def check_python_version():
    """Check Python version compatibility"""
    print("🐍 Checking Python version...")
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required!")
        print(f"   Current version: {sys.version}")
        return False
    print(f"✅ Python {sys.version.split()[0]} detected")
    return True

def check_mongodb():
    """Check if MongoDB is available"""
    print("\n🗄️  Checking MongoDB availability...")
    try:
        # Try to ping MongoDB
        result = subprocess.run(['mongod', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✅ MongoDB found locally")
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    
    print("⚠️  MongoDB not found locally")
    print("   You can:")
    print("   1. Install MongoDB locally")
    print("   2. Use MongoDB Atlas (cloud)")
    print("   3. Use Docker: docker run -d -p 27017:27017 mongo")
    return False

def create_virtual_environment():
    """Create and activate virtual environment"""
    print("\n🏗️  Setting up virtual environment...")
    
    venv_path = Path("venv")
    if venv_path.exists():
        print("✅ Virtual environment already exists")
        return True
    
    try:
        subprocess.run([sys.executable, '-m', 'venv', 'venv'], check=True)
        print("✅ Virtual environment created")
        
        # Determine activation script path
        if sys.platform == "win32":
            activate_script = venv_path / "Scripts" / "activate.bat"
            pip_path = venv_path / "Scripts" / "pip.exe"
        else:
            activate_script = venv_path / "bin" / "activate"
            pip_path = venv_path / "bin" / "pip"
        
        print(f"💡 To activate: {activate_script}")
        return True, pip_path
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to create virtual environment: {e}")
        return False, None

def install_dependencies(pip_path=None):
    """Install Python dependencies"""
    print("\n📦 Installing dependencies...")
    
    requirements_file = Path("requirements.txt")
    if not requirements_file.exists():
        print("❌ requirements.txt not found!")
        return False
    
    try:
        pip_cmd = str(pip_path) if pip_path else 'pip'
        subprocess.run([pip_cmd, 'install', '-r', 'requirements.txt'], 
                      check=True)
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        print("💡 Try: pip install -r requirements.txt")
        return False

def setup_environment_file():
    """Set up environment configuration file"""
    print("\n⚙️  Setting up environment configuration...")
    
    env_file = Path(".env")
    env_example = Path(".env.example")
    
    if env_file.exists():
        print("✅ .env file already exists")
        return True
    
    if env_example.exists():
        try:
            shutil.copy(env_example, env_file)
            print("✅ .env file created from template")
            print("💡 Please edit .env file with your actual configuration")
            return True
        except Exception as e:
            print(f"❌ Failed to copy .env.example: {e}")
    
    # Create basic .env file
    try:
        with open(env_file, 'w') as f:
            f.write("""# ETL Connector Configuration
API_BASE_URL=https://jsonplaceholder.typicode.com
API_KEY=

MONGO_URI=mongodb://localhost:27017/
MONGO_DB=etl_database
COLLECTION_NAME=jsonplaceholder_raw

RATE_LIMIT_DELAY=1.0
MAX_RETRIES=3
""")
        print("✅ Basic .env file created")
        print("💡 Please edit .env file with your configuration")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False

def setup_gitignore():
    """Ensure .gitignore is properly configured"""
    print("\n📝 Setting up Git ignore rules...")
    
    gitignore_file = Path(".gitignore")
    
    # Essential entries to ensure .env is ignored
    essential_entries = [
        ".env",
        "*.env",
        ".env.local",
        "__pycache__/",
        "*.log",
        "venv/"
    ]
    
    existing_entries = set()
    if gitignore_file.exists():
        with open(gitignore_file, 'r') as f:
            existing_entries = set(line.strip() for line in f.readlines())
    
    # Add missing essential entries
    missing_entries = [entry for entry in essential_entries 
                      if entry not in existing_entries]
    
    if missing_entries:
        with open(gitignore_file, 'a') as f:
            f.write('\n# ETL Connector - Essential ignores\n')
            for entry in missing_entries:
                f.write(f'{entry}\n')
        print(f"✅ Added {len(missing_entries)} entries to .gitignore")
    else:
        print("✅ .gitignore is properly configured")
    
    return True

def run_tests():
    """Run basic tests to verify setup"""
    print("\n🧪 Running basic tests...")
    
    try:
        # Test imports
        import requests
        import pymongo
        from dotenv import load_dotenv
        print("✅ All required modules can be imported")
        
        # Test environment loading
        load_dotenv()
        print("✅ Environment variables can be loaded")
        
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def print_next_steps():
    """Print next steps for the user"""
    print("\n🎉 Setup completed!")
    print("\n📋 Next steps:")
    print("1. Edit .env file with your configuration")
    print("2. Ensure MongoDB is running")
    print("3. Run the ETL connector:")
    
    if sys.platform == "win32":
        print("   venv\\Scripts\\activate")
    else:
        print("   source venv/bin/activate")
    
    print("   python etl_connector.py")
    print("\n4. Run tests:")
    print("   python test_connector.py")
    
    print("\n5. Git workflow:")
    print("   git add .")
    print("   git commit -m 'Initial ETL connector - [Your Name] [Roll Number]'")
    print("   git push origin your-branch-name")
    
    print("\n📚 Documentation: README.md")
    print("🔍 Logs: etl_connector.log")

def main():
    """Main setup function"""
    print_banner()
    
    success = True
    
    # Check Python version
    if not check_python_version():
        success = False
        return success
    
    # Check MongoDB
    check_mongodb()
    
    # Create virtual environment
    venv_result = create_virtual_environment()
    if isinstance(venv_result, tuple):
        venv_success, pip_path = venv_result
        if not venv_success:
            success = False
    else:
        if not venv_result:
            success = False
        pip_path = None
    
    # Install dependencies
    if success and not install_dependencies(pip_path):
        success = False
    
    # Setup environment file
    if not setup_environment_file():
        success = False
    
    # Setup gitignore
    if not setup_gitignore():
        success = False
    
    # Run basic tests
    if success and not run_tests():
        print("⚠️  Some tests failed, but setup can continue")
    
    # Print next steps
    if success:
        print_next_steps()
    else:
        print("\n❌ Setup completed with some issues")
        print("💡 Please resolve the issues above and try again")
    
    return success

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n❌ Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Setup failed with unexpected error: {e}")
        sys.exit(1)