// Copyright (c) 2016-2017 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

export class GitHubRepo {
  /**
   * @type {name} name The name of the repo.
   */
  name: String;

  get(key): any {
    return this[key];
  }
}

/* Full model:

 "hooks_url": string,
  "default_branch": string,
  "branches_url": string,
  "size": number,
  "forks_url": string,
  "forks_count": number,
  "fork": boolean,
  "git_url": string,
  "private": boolean,
  "permissions": {
    "admin": boolean,
    "push": boolean,
    "pull": boolean
  },
  "keys_url": string,
  "comments_url": string,
  "trees_url": string,
  "has_downloads": boolean,
  "blobs_url": string,
  "teams_url": string,
  "watchers_count": number,
  "stargazers_url": string,
  "stargazers_count": number,
  "contributors_url": string,
  "tags_url": string,
  "issue_comment_url": string,
  "events_url": string,
  "created_at": string,
  "issues_url": string,
  "contents_url": string,
  "merges_url": string,
  "name": string,
  "languages_url": string,
  "commits_url": string,
  "subscription_url": string,
  "clone_url": string,
  "homepage": string,
  "issue_events_url": string,
  "mirror_url": string,
  "labels_url": string,
  "url": string,
  "open_issues": number,
  "statuses_url": string,
  "forks": number,
  "archive_url": string,
  "milestones_url": string,
  "owner": {
    "gists_url": string,
    "following_url": string,
    "followers_url": string,
    "subscriptions_url": string,
    "received_events_url": string,
    "events_url": string,
    "avatar_url": string,
    "login": string,
    "url": string,
    "starred_url": string,
    "organizations_url": string,
    "repos_url": string,
    "gravatar_id": string,
    "site_admin": boolean,
    "type": string,
    "id": number,
    "html_url": string"
  },
  "assignees_url": string,
  "has_wiki": boolean,
  "compare_url": string,
  "git_tags_url": string,
  "updated_at": string,
  "watchers": number,
  "notifications_url": string",
  "ssh_url": string,
  "language": string,
  "pushed_at": string,
  "downloads_url": string,
  "subscribers_url": string,
  "id": number,
  "svn_url": string,
  "full_name": string,
  "html_url": string,
  "description": string,
  "releases_url": string,
  "git_refs_url": string,
  "collaborators_url": string,
  "pulls_url": string,
  "deployments_url": string,
  "has_projects": boolean,
  "has_pages": boolean,
  "open_issues_count": number,
  "git_commits_url": string,
  "has_issues": boolean

  */
