# encoding: utf-8
Gem::Specification.new do |spec|
  spec.add_dependency 'ci_reporter', ['~> 1.7.0']
  spec.add_dependency 'fastercsv', ['~> 1.5.3']
  spec.add_dependency 'highline', ['~> 1.6.11']
  spec.add_dependency 'allplayers', ['~> 0.1.0']
  spec.authors = ["AllPlayers.com"]
  spec.description = %q{A Ruby tool to handle import spreadsheets into AllPlayers API.}
  spec.email = ['support@allplayers.com']
  spec.files = %w(README.md allplayers_imports.gemspec)
  spec.files += Dir.glob("lib/**/*.rb")
  spec.homepage = 'http://www.allplayers.com/'
  spec.licenses = ['MIT']
  spec.name = 'allplayers_imports'
  spec.require_paths = ['lib']
  spec.required_rubygems_version = Gem::Requirement.new('>= 1.3.6')
  spec.summary = spec.description
  spec.version = '0.1.0'
end