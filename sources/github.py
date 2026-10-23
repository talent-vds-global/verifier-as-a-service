import requests
import config

def get_pr_diff(repo: str, pr_number: int) -> str:
    """Gọi GitHub API lấy danh sách file thay đổi của PR và ghép các patch thành chuỗi diff hoàn chỉnh.

    Args:
        repo (str): Tên repository dạng 'owner/repo' (ví dụ: 'octocat/Hello-World').
        pr_number (int): Số thứ tự của Pull Request.

    Returns:
        str: Chuỗi diff tổng hợp của tất cả các file trong PR.
    """
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "agent-review-bot"
    }

    if config.GITHUB_TOKEN:
        headers["Authorization"] = f"token {config.GITHUB_TOKEN}"

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        files = response.json()

        diff_chunks = []
        for file in files:
            filename = file.get("filename", "unknown")
            status = file.get("status", "modified")
            patch = file.get("patch", "")
            diff_chunks.append(f"### File: {filename} ({status})\n```diff\n{patch}\n```")

        if not diff_chunks:
            return "Không tìm thấy thay đổi nào (diff trống) trong Pull Request này."

        return "\n\n".join(diff_chunks)
    except requests.exceptions.RequestException as e:
        return f"Lỗi khi lấy PR diff từ GitHub API ({url}): {str(e)}"
