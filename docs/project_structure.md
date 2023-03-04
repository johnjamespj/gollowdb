# GO Project Structure

## Go Directories

* <code>/internal</code> : Private application and library code. This is the code you don't want others importing in their applications or libraries.
* <code>/pkg</code> : Library code that's ok to use by external applications (e.g., /pkg/mypubliclib). Other projects will import these libraries expecting them to work, so think twice before you put something here :-) Note that the internal directory is a better way to ensure your private packages are not importable because it's enforced by Go. 
* <code>/docs</code> : Design and user documents (in addition to your godoc generated documentation).
* <code>/tests</code> : Sources for tests

Source : https://github.com/golang-standards/project-layout
